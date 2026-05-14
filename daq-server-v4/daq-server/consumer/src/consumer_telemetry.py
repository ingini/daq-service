"""
consumer_telemetry.py
v.telemetry topic consumer

저장 경로:
  sensor: ingest-bucket/{yyyy}/{mm}/{dd}/{vid}/sensor/{imu|gnss|can}/{first_ts}_{last_ts}.parquet

idempotent 원칙:
  - parquet 키 = 버퍼 내 first_hw_ts ~ last_hw_ts
  - 동일 구간 재처리 시 동일 키 → MinIO overwrite → 중복 없음
  - offset commit: flush 성공 후에만
"""
import os
import io
import time
import logging
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError, TopicPartition
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from proto.telemetry_pb2 import ImuFrame, GnssFrame, CanFrame

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("consumer-telemetry")

KAFKA_BOOTSTRAP  = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_GROUP      = os.environ["KAFKA_GROUP_TELEMETRY"]
TOPIC            = os.environ["TOPIC_TELEMETRY"]
MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"].replace("http://", "")
MINIO_ACCESS     = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET     = os.environ["MINIO_SECRET_KEY"]
BUCKET           = os.environ["MINIO_BUCKET"]
FLUSH_ROWS       = int(os.environ.get("PARQUET_FLUSH_ROWS", 1000))
FLUSH_SECONDS    = int(os.environ.get("PARQUET_FLUSH_SECONDS", 60))
MAX_PENDING_ROWS = int(os.environ.get("MAX_PENDING_ROWS", 5000))

IMU_SCHEMA = pa.schema([
    pa.field("vehicle_id",  pa.string()),
    pa.field("hw_ts_us",    pa.int64()),
    pa.field("recv_ts_us",  pa.int64()),
    pa.field("ax",          pa.float64()),
    pa.field("ay",          pa.float64()),
    pa.field("az",          pa.float64()),
    pa.field("gx",          pa.float64()),
    pa.field("gy",          pa.float64()),
    pa.field("gz",          pa.float64()),
])

GNSS_SCHEMA = pa.schema([
    pa.field("vehicle_id",  pa.string()),
    pa.field("hw_ts_us",    pa.int64()),
    pa.field("recv_ts_us",  pa.int64()),
    pa.field("lat",         pa.float64()),
    pa.field("lon",         pa.float64()),
    pa.field("alt",         pa.float64()),
    pa.field("heading",     pa.float64()),
    pa.field("speed",       pa.float64()),
    pa.field("fix_type",    pa.int32()),
    pa.field("satellites",  pa.int32()),
])

CAN_SCHEMA = pa.schema([
    pa.field("vehicle_id",  pa.string()),
    pa.field("hw_ts_us",    pa.int64()),
    pa.field("recv_ts_us",  pa.int64()),
    pa.field("frame_id",    pa.int32()),
    pa.field("data",        pa.binary()),
])


class ParquetBuffer:
    """
    sensor_type + vehicle_id 기준 Parquet 버퍼.

    parquet 키: {first_hw_ts}_{last_hw_ts}.parquet
    → 동일 구간 재처리 시 동일 키 → MinIO overwrite → 중복 없음
    """

    def __init__(self, schema: pa.Schema, sensor_type: str,
                 vehicle_id: str, minio: Minio, bucket: str):
        self.schema          = schema
        self.sensor_type     = sensor_type
        self.vehicle_id      = vehicle_id
        self.minio           = minio
        self.bucket          = bucket
        self.rows            = []
        self.pending_offsets = []
        self.last_flush      = time.time()
        self.first_hw_ts     = None
        self.last_hw_ts      = None
        self.flush_time      = None

    def append(self, row: dict, partition: int, offset: int) -> list:
        if self.first_hw_ts is None:
            self.first_hw_ts = row["hw_ts_us"]
            self.flush_time  = datetime.now(timezone.utc)
        self.last_hw_ts = row["hw_ts_us"]

        self.rows.append(row)
        self.pending_offsets.append((partition, offset))

        need_flush = (
            len(self.rows) >= FLUSH_ROWS or
            time.time() - self.last_flush >= FLUSH_SECONDS or
            len(self.rows) >= MAX_PENDING_ROWS
        )
        return self.flush() if need_flush else []

    def flush(self) -> list:
        if not self.rows:
            return []
        try:
            table = pa.Table.from_pylist(self.rows, schema=self.schema)
            buf   = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            buf.seek(0)

            # 키: 구간 first_hw_ts ~ last_hw_ts
            # 재처리 시 동일 구간 → 동일 키 → overwrite
            now = self.flush_time or datetime.now(timezone.utc)
            key = (f"{now.strftime('%Y/%m/%d')}/{self.vehicle_id}/sensor/"
                   f"{self.sensor_type}/"
                   f"{self.first_hw_ts}_{self.last_hw_ts}.parquet")

            self.minio.put_object(
                self.bucket, key,
                buf, length=buf.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            log.info(f"[parquet flush] {key} ({len(self.rows)} rows)")

            committed = list(self.pending_offsets)
            self.rows.clear()
            self.pending_offsets.clear()
            self.first_hw_ts = None
            self.last_hw_ts  = None
            self.flush_time  = None
            self.last_flush  = time.time()
            return committed

        except Exception as e:
            log.error(f"[parquet flush] {self.sensor_type} 실패 (데이터 유지, 재시도 예정): {e}")
            self.last_flush = time.time()
            return []


def commit_offsets(consumer: Consumer, offsets: list):
    if not offsets:
        return
    max_per_partition = {}
    for partition, offset in offsets:
        if partition not in max_per_partition or offset > max_per_partition[partition]:
            max_per_partition[partition] = offset
    tp_list = [
        TopicPartition(TOPIC, p, o + 1)
        for p, o in max_per_partition.items()
    ]
    consumer.commit(offsets=tp_list, asynchronous=False)
    log.debug(f"[commit] {tp_list}")


def main():
    minio = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False
    )
    if not minio.bucket_exists(BUCKET):
        minio.make_bucket(BUCKET)
        log.info(f"bucket 생성: {BUCKET}")

    # key: "{vid}/{sensor_type}" → ParquetBuffer
    buffers: dict[str, ParquetBuffer] = {}

    def get_buf(vid: str, sensor_type: str, schema: pa.Schema) -> ParquetBuffer:
        k = f"{vid}/{sensor_type}"
        if k not in buffers:
            buffers[k] = ParquetBuffer(schema, sensor_type, vid, minio, BUCKET)
        return buffers[k]

    consumer = Consumer({
        "bootstrap.servers":    KAFKA_BOOTSTRAP,
        "group.id":             KAFKA_GROUP,
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms":   30000,
    })
    consumer.subscribe([TOPIC])
    log.info(f"[telemetry consumer] 시작. topic={TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                for buf in list(buffers.values()):
                    committed = buf.flush()
                    commit_offsets(consumer, committed)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Kafka error: {msg.error()}")
                continue

            key   = msg.key().decode() if msg.key() else ""
            value = msg.value()

            try:
                committed = []

                if "/imu" in key:
                    f = ImuFrame()
                    f.ParseFromString(value)
                    committed = get_buf(f.vehicle_id, "imu", IMU_SCHEMA).append({
                        "vehicle_id": f.vehicle_id,
                        "hw_ts_us": f.hw_ts_us, "recv_ts_us": f.recv_ts_us,
                        "ax": f.ax, "ay": f.ay, "az": f.az,
                        "gx": f.gx, "gy": f.gy, "gz": f.gz,
                    }, msg.partition(), msg.offset())

                elif "/gnss" in key:
                    f = GnssFrame()
                    f.ParseFromString(value)
                    committed = get_buf(f.vehicle_id, "gnss", GNSS_SCHEMA).append({
                        "vehicle_id": f.vehicle_id,
                        "hw_ts_us": f.hw_ts_us, "recv_ts_us": f.recv_ts_us,
                        "lat": f.lat, "lon": f.lon, "alt": f.alt,
                        "heading": f.heading, "speed": f.speed,
                        "fix_type": f.fix_type, "satellites": f.satellites,
                    }, msg.partition(), msg.offset())

                elif "/can" in key:
                    f = CanFrame()
                    f.ParseFromString(value)
                    committed = get_buf(f.vehicle_id, "can", CAN_SCHEMA).append({
                        "vehicle_id": f.vehicle_id,
                        "hw_ts_us": f.hw_ts_us, "recv_ts_us": f.recv_ts_us,
                        "frame_id": f.frame_id, "data": f.data,
                    }, msg.partition(), msg.offset())

                else:
                    consumer.commit(
                        offsets=[TopicPartition(TOPIC, msg.partition(), msg.offset() + 1)],
                        asynchronous=False
                    )
                    continue

                commit_offsets(consumer, committed)

            except Exception as e:
                log.error(f"파싱 실패 key={key}: {e}")
                consumer.commit(
                    offsets=[TopicPartition(TOPIC, msg.partition(), msg.offset() + 1)],
                    asynchronous=False
                )

    except KeyboardInterrupt:
        log.info("종료 중...")
    finally:
        for buf in buffers.values():
            committed = buf.flush()
            commit_offsets(consumer, committed)
        consumer.close()


if __name__ == "__main__":
    main()
