"""
consumer_sensor.py
v.sensor topic consumer

저장 경로:
  early-fusion cam:   ingest-bucket/{yyyy}/{mm}/{dd}/{vid}/early-fusion/cam/{hh}/{mm}/{sensor}_{ts}_{seq}.h264
  early-fusion lidar: ingest-bucket/{yyyy}/{mm}/{dd}/{vid}/early-fusion/lidar/{hh}/{mm}/{sensor}_{ts}_{seq}.pcap
  mcap:               ingest-bucket/{yyyy}/{mm}/{dd}/{vid}/mcap/{hh}/{mm}/{first_ts}_{last_ts}.mcap

idempotent 원칙:
  - raw: 키 = sensor + hw_ts + seq → 동일 메시지 재처리 시 overwrite
  - mcap: 키 = 구간 first_hw_ts ~ last_hw_ts → 재처리 시 overwrite
  - offset commit: flush 성공 후에만
"""
import os
import io
import struct
import logging
import time
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError, TopicPartition
from minio import Minio
from mcap.writer import Writer as McapWriter
from proto.sensor_pb2 import SensorHeader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("consumer-sensor")

KAFKA_BOOTSTRAP    = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_GROUP        = os.environ["KAFKA_GROUP_SENSOR"]
TOPIC              = os.environ["TOPIC_SENSOR"]
MINIO_ENDPOINT     = os.environ["MINIO_ENDPOINT"].replace("http://", "")
MINIO_ACCESS       = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET       = os.environ["MINIO_SECRET_KEY"]
BUCKET             = os.environ["MINIO_BUCKET"]
MCAP_FLUSH_FRAMES  = int(os.environ.get("MCAP_FLUSH_FRAMES", 300))
MCAP_FLUSH_SECONDS = int(os.environ.get("MCAP_FLUSH_SECONDS", 30))
MAX_PENDING_FRAMES = int(os.environ.get("MAX_PENDING_FRAMES", 1000))


def date_path(now: datetime) -> str:
    return now.strftime("%Y/%m/%d")


def hhmm(now: datetime) -> str:
    return now.strftime("%H/%M")


def parse_sensor_message(raw: bytes):
    hdr_len = struct.unpack(">I", raw[:4])[0]
    hdr = SensorHeader()
    hdr.ParseFromString(raw[4:4 + hdr_len])
    payload = raw[4 + hdr_len:]
    return hdr, payload


class McapBuffer:
    """
    vehicle_id 기준 MCAP 버퍼.

    mcap 키: {first_hw_ts}_{last_hw_ts}.mcap
    → 동일 구간 재처리 시 동일 키 → MinIO overwrite → 중복 없음
    """

    def __init__(self, vehicle_id: str, minio: Minio, bucket: str):
        self.vehicle_id      = vehicle_id
        self.minio           = minio
        self.bucket          = bucket
        self.frame_count     = 0
        self.last_flush      = time.time()
        self.pending_offsets = []
        self.first_hw_ts     = None   # 구간 시작 ts
        self.last_hw_ts      = None   # 구간 종료 ts
        self.flush_time      = None   # flush 시점 now() (경로용)
        self._reset_writer()

    def _reset_writer(self):
        self.buf      = io.BytesIO()
        self.writer   = McapWriter(self.buf)
        self.writer.start(profile="", library="daq-server")
        self.channels = {}

    def _get_channel(self, sensor_name: str, codec: str) -> int:
        if sensor_name not in self.channels:
            schema_id = self.writer.register_schema(
                name=sensor_name, encoding="protobuf", data=b"")
            ch_id = self.writer.register_channel(
                topic=f"/{sensor_name}",
                message_encoding=codec,
                schema_id=schema_id,
                metadata={"sensor": sensor_name, "codec": codec})
            self.channels[sensor_name] = ch_id
        return self.channels[sensor_name]

    def append(self, hdr: SensorHeader, payload: bytes,
               partition: int, offset: int) -> list:
        # 구간 ts 추적
        if self.first_hw_ts is None:
            self.first_hw_ts = hdr.hw_ts_us
            self.flush_time  = datetime.now(timezone.utc)
        self.last_hw_ts = hdr.hw_ts_us

        self._get_channel(hdr.sensor_name, hdr.codec)
        self.writer.add_message(
            channel_id=self.channels[hdr.sensor_name],
            log_time=hdr.hw_ts_us * 1000,
            publish_time=hdr.recv_ts_us * 1000,
            sequence=hdr.frame_seq,
            data=payload,
        )
        self.frame_count += 1
        self.pending_offsets.append((partition, offset))

        need_flush = (
            self.frame_count >= MCAP_FLUSH_FRAMES or
            time.time() - self.last_flush >= MCAP_FLUSH_SECONDS or
            len(self.pending_offsets) >= MAX_PENDING_FRAMES
        )
        return self.flush() if need_flush else []

    def flush(self) -> list:
        if self.frame_count == 0:
            return []
        try:
            self.writer.finish()
            self.buf.seek(0)
            data = self.buf.read()

            # 키: 구간 first_hw_ts ~ last_hw_ts
            # 재처리 시 동일 구간 → 동일 키 → overwrite
            now = self.flush_time or datetime.now(timezone.utc)
            key = (f"{date_path(now)}/{self.vehicle_id}/mcap/"
                   f"{hhmm(now)}/"
                   f"{self.first_hw_ts}_{self.last_hw_ts}.mcap")

            self.minio.put_object(
                self.bucket, key,
                io.BytesIO(data), length=len(data),
                content_type="application/octet-stream"
            )
            log.info(f"[mcap flush] {key} ({self.frame_count} frames)")

            committed = list(self.pending_offsets)
            self.pending_offsets = []
            self.frame_count     = 0
            self.first_hw_ts     = None
            self.last_hw_ts      = None
            self.flush_time      = None
            self.last_flush      = time.time()
            self._reset_writer()
            return committed

        except Exception as e:
            log.error(f"[mcap flush] 실패 (데이터 유지, 재시도 예정): {e}")
            # writer 재초기화만, 데이터/offsets/ts 유지
            self._reset_writer()
            self.frame_count = 0
            self.last_flush  = time.time()
            return []


def upload_raw(minio: Minio, bucket: str,
               hdr: SensorHeader, payload: bytes) -> bool:
    """
    키: {sensor}_{hw_ts_us}_{frame_seq}.{ext}
    → 동일 메시지 재처리 시 동일 키 → overwrite → 중복 없음
    """
    now     = datetime.now(timezone.utc)
    is_cam  = hdr.codec == "h264"
    subtype = "cam" if is_cam else "lidar"
    ext     = "h264" if is_cam else "pcap"

    key = (f"{date_path(now)}/{hdr.vehicle_id}/early-fusion/"
           f"{subtype}/{hhmm(now)}/"
           f"{hdr.sensor_name}_{hdr.hw_ts_us}_{hdr.frame_seq}.{ext}")
    try:
        minio.put_object(
            bucket, key,
            io.BytesIO(payload), length=len(payload),
            content_type="application/octet-stream"
        )
        return True
    except Exception as e:
        log.error(f"[raw upload] {key} 실패: {e}")
        return False


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

    mcap_buffers: dict[str, McapBuffer] = {}

    def get_mcap_buf(vehicle_id: str) -> McapBuffer:
        if vehicle_id not in mcap_buffers:
            mcap_buffers[vehicle_id] = McapBuffer(vehicle_id, minio, BUCKET)
            log.info(f"[mcap] 새 버퍼: {vehicle_id}")
        return mcap_buffers[vehicle_id]

    consumer = Consumer({
        "bootstrap.servers":         KAFKA_BOOTSTRAP,
        "group.id":                  KAFKA_GROUP,
        "auto.offset.reset":         "earliest",
        "enable.auto.commit":        False,
        "max.poll.interval.ms":      600000,
        "fetch.max.bytes":           10485760,
        "max.partition.fetch.bytes": 10485760,
    })
    consumer.subscribe([TOPIC])
    log.info(f"[sensor consumer] 시작. topic={TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                for buf in list(mcap_buffers.values()):
                    committed = buf.flush()
                    commit_offsets(consumer, committed)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Kafka error: {msg.error()}")
                continue

            try:
                hdr, payload = parse_sensor_message(msg.value())
            except Exception as e:
                log.error(f"메시지 파싱 실패: {e}")
                consumer.commit(
                    offsets=[TopicPartition(TOPIC, msg.partition(), msg.offset() + 1)],
                    asynchronous=False
                )
                continue

            if not upload_raw(minio, BUCKET, hdr, payload):
                log.warning("raw upload 실패 → offset commit 스킵 (재처리)")
                continue

            committed = get_mcap_buf(hdr.vehicle_id).append(
                hdr, payload, msg.partition(), msg.offset()
            )
            commit_offsets(consumer, committed)

    except KeyboardInterrupt:
        log.info("종료 중...")
    finally:
        for buf in mcap_buffers.values():
            committed = buf.flush()
            commit_offsets(consumer, committed)
        consumer.close()


if __name__ == "__main__":
    main()
