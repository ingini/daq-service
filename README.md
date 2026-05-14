# daq-server

자율주행 차량 센서 데이터 수신 → 저장 서버

```
daq-edge (차량 최대 15대)
    ↓ Kafka (v.telemetry / v.sensor)
daq-server
    ├─ Kafka Broker  (192.168.1.81)
    ├─ Consumer      (192.168.1.92)
    └─ MinIO         (192.168.1.150)
         └─ ingest-bucket
```

---

## 서버 구성

| IP | 역할 | k8s label |
|----|------|-----------|
| 192.168.1.12 | Control Plane | - |
| 192.168.1.81 | Worker: Kafka | daq-role=kafka |
| 192.168.1.92 | Worker: Consumer | daq-role=consumer |
| 192.168.1.71 | Worker: GPU (예비, NoSchedule) | daq-role=gpu |
| 192.168.1.79 | Worker: GPU (예비, NoSchedule) | daq-role=gpu |
| 192.168.1.150 | MinIO (k8s 외부) | - |

---

## 디렉토리 구조

```
daq-server/
├── scripts/
│   ├── 00_node_setup.sh
│   ├── 01_init_master.sh
│   ├── 02_join_workers.sh
│   ├── 03_deploy.sh
│   └── scale.sh
├── k8s/
│   ├── kafka/
│   │   ├── 00_storage_class.yaml
│   │   ├── 01_kafka_config.yaml
│   │   ├── 02_kafka_statefulset.yaml
│   │   └── 03_kafka_topic_init.yaml
│   ├── consumer/
│   │   ├── 01_consumer_config.yaml
│   │   └── 02_consumer_deployment.yaml
│   └── monitoring/
│       └── kafka_ui.yaml
└── consumer/
    ├── src/
    │   ├── consumer_telemetry.py
    │   └── consumer_sensor.py
    ├── requirements.txt
    └── Dockerfile
```

---

## 1. k8s 클러스터 설치

### Step 1 - 모든 노드 (12, 81, 92, 71, 79)

```bash
chmod +x scripts/00_node_setup.sh
sudo ./scripts/00_node_setup.sh
```

### Step 2 - Control Plane (192.168.1.12) 만

```bash
chmod +x scripts/01_init_master.sh
sudo ./scripts/01_init_master.sh
```

출력 마지막 join command 복사:
```
kubeadm join 192.168.1.12:6443 --token xxxx --discovery-token-ca-cert-hash sha256:xxxx
```

### Step 3 - Worker 노드 (81, 92, 71, 79) 각각

```bash
sudo kubeadm join 192.168.1.12:6443 --token xxxx --discovery-token-ca-cert-hash sha256:xxxx
```

### Step 4 - Control Plane에서 label 부여

```bash
chmod +x scripts/02_join_workers.sh
./scripts/02_join_workers.sh
```

### 확인

```bash
kubectl get nodes -o wide
```

---

## 2. MinIO 버킷 생성 (1회)

```bash
mc alias set minio http://192.168.1.150:9000 ACCESS_KEY SECRET_KEY
mc mb minio/ingest-bucket
mc ls minio
```

---

## 3. Consumer 이미지 빌드

### proto 파일 준비

```bash
mkdir -p consumer/proto
cp /path/to/daq-edge/common/proto/telemetry.proto consumer/proto/
cp /path/to/daq-edge/common/proto/sensor.proto    consumer/proto/
```

### 빌드

```bash
docker build -t daq-consumer:latest ./consumer
```

### worker 노드 배포 (레지스트리 없을 경우)

```bash
docker save daq-consumer:latest | gzip > daq-consumer.tar.gz
scp daq-consumer.tar.gz user@192.168.1.92:~/
ssh user@192.168.1.92 "docker load < ~/daq-consumer.tar.gz"
```

---

## 4. Secret 설정

```bash
kubectl create secret generic consumer-secret \
  --namespace daq \
  --from-literal=MINIO_ACCESS_KEY=실제키 \
  --from-literal=MINIO_SECRET_KEY=실제시크릿 \
  --dry-run=client -o yaml | kubectl apply -f -
```

---

## 5. 전체 배포

```bash
chmod +x scripts/03_deploy.sh
./scripts/03_deploy.sh
```

### 단계별 수동 배포

```bash
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.26/deploy/local-path-storage.yaml

kubectl apply -f k8s/kafka/01_kafka_config.yaml
kubectl apply -f k8s/kafka/02_kafka_statefulset.yaml
kubectl wait --namespace daq --for=condition=ready pod --selector=app=kafka --timeout=180s
kubectl apply -f k8s/kafka/03_kafka_topic_init.yaml

kubectl apply -f k8s/consumer/01_consumer_config.yaml
kubectl apply -f k8s/consumer/02_consumer_deployment.yaml
kubectl apply -f k8s/monitoring/kafka_ui.yaml
```

---

## 6. 배포 확인

```bash
kubectl get pods -n daq
# kafka-0                  1/1  Running  server-81
# consumer-telemetry-xxx   1/1  Running  server-92  (8개)
# consumer-sensor-xxx      1/1  Running  server-92  (16개)
# kafka-ui-xxx             1/1  Running  server-81

http://192.168.1.81:30080   # Kafka UI

kubectl logs -n daq -l app=consumer-sensor    -f
kubectl logs -n daq -l app=consumer-telemetry -f
```

---

## 7. daq-edge 연결

차량 `collector.yaml`:
```yaml
kafka:
  bootstrap_servers: "192.168.1.81:9094"
```

---

## 8. Kafka 토픽 구성

| 토픽 | 파티션 | 보관 | 용도 |
|------|--------|------|------|
| v.telemetry | 8 | **1일** | IMU / GNSS / CAN |
| v.sensor | 16 | **1일** | LiDAR / Camera |

### 파티션 수 기준

파티션 수 = 동시에 처리할 수 있는 최대 consumer pod 수.

```
v.sensor 파티션 16
→ consumer-sensor pod 16개 = 파티션 1개씩 전담 = 최대 병렬
→ consumer-sensor pod 8개  = 파티션 2개씩 = 절반 성능
→ pod 수 > 파티션 수       = 초과 pod idle (의미 없음)
```

### 파티션 수 변경 (늘리기만 가능, 줄이기 불가)

```bash
kubectl exec -n daq kafka-0 -- \
  kafka-topics --alter \
  --bootstrap-server localhost:9092 \
  --topic v.sensor \
  --partitions 32
```

---

## 9. MinIO 저장 구조

```
ingest-bucket/
└── {yyyy}/
    └── {mm}/
        └── {dd}/
            └── {VID}/
                ├── early-fusion/
                │   ├── cam/
                │   │   └── {hh}/{mm}/
                │   │       └── {sensor}_{hw_ts}_{seq}.h264
                │   └── lidar/
                │       └── {hh}/{mm}/
                │           └── {sensor}_{hw_ts}_{seq}.pcap
                ├── mcap/
                │   └── {hh}/{mm}/
                │       └── {first_hw_ts}_{last_hw_ts}.mcap
                └── sensor/
                    ├── imu/
                    │   └── {first_hw_ts}_{last_hw_ts}.parquet
                    ├── gnss/
                    │   └── {first_hw_ts}_{last_hw_ts}.parquet
                    └── can/
                        └── {first_hw_ts}_{last_hw_ts}.parquet
```

### 실제 키 예시

```
2026/05/14/U100-AP500-01/early-fusion/cam/09/30/cam_front_left_down_1715000000123456_0.h264
2026/05/14/U100-AP500-01/early-fusion/lidar/09/30/eth_e_a_1715000000123456_0.pcap
2026/05/14/U100-AP500-01/mcap/09/30/1715000000100000_1715000000399000.mcap
2026/05/14/U100-AP500-01/sensor/imu/1715000000100000_1715000001100000.parquet
```

---

## 10. Idempotent 저장 전략 (중복 방지)

재처리 시 중복이 발생하지 않도록 파일 키를 고정값으로 설계.

| 데이터 | 키 구성 | 재처리 시 |
|--------|---------|---------|
| early-fusion | `{sensor}_{hw_ts}_{seq}.ext` | 동일 키 → overwrite |
| mcap | `{first_hw_ts}_{last_hw_ts}.mcap` | 동일 구간 → overwrite |
| parquet | `{first_hw_ts}_{last_hw_ts}.parquet` | 동일 구간 → overwrite |

```
정상 처리:
offset 10~309 → mcap flush → 1715000000100000_1715000000399000.mcap ✅

pod 재시작 → offset 10부터 재처리:
동일 데이터 → 동일 키 → MinIO overwrite
→ 중복 파일 없음 ✅
→ 내용 동일 ✅
```

---

## 11. Consumer 동작 원칙

### pod = 파티션 수 (최대 병렬)

```
consumer-sensor pod 0  → partition 0  전담
consumer-sensor pod 1  → partition 1  전담
...
consumer-sensor pod 15 → partition 15 전담

각 pod 독립 동기 처리 → offset 꼬임 없음
MinIO write 16개 동시 실행
```

### offset commit 흐름

```
raw upload 성공
    ↓
mcap / parquet flush 성공
    ↓
offset commit
(실패 시 commit 안 함 → 재시작 후 재처리)
```

### 환경변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| MCAP_FLUSH_FRAMES | 300 | N프레임마다 flush |
| MCAP_FLUSH_SECONDS | 30 | N초마다 flush |
| MAX_PENDING_FRAMES | 1000 | 메모리 누적 방지 상한 |
| PARQUET_FLUSH_ROWS | 1000 | N행마다 flush |
| PARQUET_FLUSH_SECONDS | 60 | N초마다 flush |
| MAX_PENDING_ROWS | 5000 | 메모리 누적 방지 상한 |

---

## 12. 스케일 조정

```bash
./scripts/scale.sh up        # 최대 (sensor 16, telemetry 8)
./scripts/scale.sh down      # 최소 (sensor 4, telemetry 2)
./scripts/scale.sh set 12    # sensor 12개 지정
./scripts/scale.sh status
./scripts/scale.sh restart
```

---

## 13. GPU worker 활성화 (추후 Annotation)

```bash
kubectl taint node server-71 gpu=annotation:NoSchedule-
kubectl taint node server-79 gpu=annotation:NoSchedule-
```

---

## 14. 트러블슈팅

### Kafka pod 안 뜸

```bash
kubectl describe pod kafka-0 -n daq
kubectl get pvc -n daq
kubectl get sc
```

### Consumer lag 확인

```bash
kubectl exec -n daq kafka-0 -- \
  kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group daq-consumer-sensor
```

### MinIO 연결 확인

```bash
kubectl exec -n daq -it \
  $(kubectl get pod -n daq -l app=consumer-sensor -o name | head -1) -- \
  python -c "
from minio import Minio
m = Minio('192.168.1.150:9000', access_key='키', secret_key='시크릿', secure=False)
print([b.name for b in m.list_buckets()])
"
```

### pod OOM 발생 시

```bash
kubectl describe pod -n daq -l app=consumer-sensor | grep -A5 OOM
# k8s/consumer/02_consumer_deployment.yaml 에서 limits.memory 상향 후
kubectl apply -f k8s/consumer/02_consumer_deployment.yaml
```

### 노드에 pod 안 올라감

```bash
kubectl get nodes --show-labels | grep daq-role
kubectl label node server-92 daq-role=consumer --overwrite
```
