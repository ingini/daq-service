#!/bin/bash
# ============================================================
# 02_join_workers.sh
# Control Plane (192.168.1.12) 에서 실행
# worker 노드들 join 후 label 부여
# ============================================================
set -e

# ── join 토큰 생성 ──────────────────────────────────────────
JOIN_CMD=$(kubeadm token create --print-join-command)
echo "[02] join command:"
echo "  $JOIN_CMD"
echo ""
echo "각 worker 노드(81, 92, 71, 79)에서 위 명령어 실행 후 돌아오세요."
echo "엔터 누르면 label 설정 진행"
read

# ── 노드 확인 ──────────────────────────────────────────────
kubectl get nodes

# ── label 부여 ─────────────────────────────────────────────
echo "[02] label 부여"

# Kafka + Consumer
kubectl label node $(kubectl get node -o wide | grep 192.168.1.81 | awk '{print $1}') \
  node-role.kubernetes.io/worker='' \
  daq-role=kafka \
  --overwrite

# Consumer + Serving
kubectl label node $(kubectl get node -o wide | grep 192.168.1.92 | awk '{print $1}') \
  node-role.kubernetes.io/worker='' \
  daq-role=consumer \
  --overwrite

# GPU Annotation (지금은 NoSchedule, 나중에 해제)
kubectl label node $(kubectl get node -o wide | grep 192.168.1.71 | awk '{print $1}') \
  node-role.kubernetes.io/worker='' \
  daq-role=gpu \
  gpu-type=2080ti \
  --overwrite
kubectl taint node $(kubectl get node -o wide | grep 192.168.1.71 | awk '{print $1}') \
  gpu=annotation:NoSchedule

kubectl label node $(kubectl get node -o wide | grep 192.168.1.79 | awk '{print $1}') \
  node-role.kubernetes.io/worker='' \
  daq-role=gpu \
  gpu-type=3080 \
  --overwrite
kubectl taint node $(kubectl get node -o wide | grep 192.168.1.79 | awk '{print $1}') \
  gpu=annotation:NoSchedule

echo "[02] 완료"
kubectl get nodes --show-labels
