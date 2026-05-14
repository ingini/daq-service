#!/bin/bash
# ============================================================
# 03_deploy.sh
# Control Plane (192.168.1.12) 에서 실행
# 전체 daq-server 배포
# ============================================================
set -e

REGISTRY="192.168.1.12:5000"   # 사내 레지스트리 (없으면 dockerhub)
VERSION=${1:-latest}

echo "================================================"
echo " DAQ Server 배포  version=$VERSION"
echo "================================================"

# ── 0. local-path provisioner 설치 ──────────────────────────
echo "[1/6] StorageClass 설치"
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.26/deploy/local-path-storage.yaml

# ── 1. namespace + Kafka 설정 ────────────────────────────────
echo "[2/6] Kafka 설치"
kubectl apply -f k8s/kafka/01_kafka_config.yaml
kubectl apply -f k8s/kafka/02_kafka_statefulset.yaml

echo "  Kafka 기동 대기 중..."
kubectl wait --namespace daq \
  --for=condition=ready pod \
  --selector=app=kafka \
  --timeout=180s

# ── 2. 토픽 생성 ─────────────────────────────────────────────
echo "[3/6] Kafka 토픽 생성"
kubectl apply -f k8s/kafka/03_kafka_topic_init.yaml
kubectl wait --namespace daq \
  --for=condition=complete job/kafka-topic-init \
  --timeout=120s

# ── 3. Consumer 이미지 빌드 + 푸시 ──────────────────────────
echo "[4/6] Consumer 이미지 빌드"
# proto 파일 복사 (daq-edge 공통)
mkdir -p consumer/proto
cp ../daq-edge/common/proto/*.proto consumer/proto/ 2>/dev/null || true

docker build -t daq-consumer:$VERSION ./consumer
docker tag daq-consumer:$VERSION $REGISTRY/daq-consumer:$VERSION
docker push $REGISTRY/daq-consumer:$VERSION || echo "  (레지스트리 없으면 각 노드에 직접 load)"

# ── 4. Consumer 배포 ─────────────────────────────────────────
echo "[5/6] Consumer 배포"
kubectl apply -f k8s/consumer/01_consumer_config.yaml
kubectl apply -f k8s/consumer/02_consumer_deployment.yaml

# ── 5. 모니터링 ──────────────────────────────────────────────
echo "[6/6] Kafka UI 배포"
kubectl apply -f k8s/monitoring/kafka_ui.yaml

echo ""
echo "================================================"
echo " 배포 완료"
echo "================================================"
echo ""
echo " Kafka UI:   http://192.168.1.81:30080"
echo " Kafka:      192.168.1.81:9094 (daq-edge 접속)"
echo ""
echo " 상태 확인:"
echo "   kubectl get pods -n daq"
echo "   kubectl logs -n daq -l app=consumer-sensor -f"
