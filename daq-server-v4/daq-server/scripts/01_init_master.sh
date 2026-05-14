#!/bin/bash
# ============================================================
# 01_init_master.sh
# Control Plane (192.168.1.12) 에서만 실행
# ============================================================
set -e

MASTER_IP="192.168.1.12"
POD_CIDR="10.244.0.0/16"   # Flannel 기본값

echo "[01] kubeadm init"
kubeadm init \
  --apiserver-advertise-address=$MASTER_IP \
  --pod-network-cidr=$POD_CIDR \
  --kubernetes-version=v1.29.0

echo "[01] kubeconfig 설정"
mkdir -p $HOME/.kube
cp /etc/kubernetes/admin.conf $HOME/.kube/config
chown $(id -u):$(id -g) $HOME/.kube/config

echo "[01] CNI - Flannel 설치"
kubectl apply -f \
  https://raw.githubusercontent.com/flannel-io/flannel/master/Documentation/kube-flannel.yml

echo "[01] 노드 확인"
kubectl get nodes

echo ""
echo "=========================================="
echo "worker 노드에서 아래 명령어 실행하세요:"
echo "(kubeadm join 토큰은 아래 명령으로 재발급)"
echo "  kubeadm token create --print-join-command"
echo "=========================================="
kubeadm token create --print-join-command
