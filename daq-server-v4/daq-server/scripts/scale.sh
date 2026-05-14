#!/bin/bash
# ============================================================
# scale.sh - consumer pod 수 조정
# 사용법:
#   ./scale.sh up    → sensor consumer 8개로 증가
#   ./scale.sh down  → sensor consumer 2개로 감소
#   ./scale.sh set 6 → 지정 수로 변경
# ============================================================

ACTION=${1:-status}
COUNT=${2:-4}

case $ACTION in
  up)
    kubectl scale deployment consumer-sensor -n daq --replicas=8
    kubectl scale deployment consumer-telemetry -n daq --replicas=4
    echo "스케일 업 완료"
    ;;
  down)
    kubectl scale deployment consumer-sensor -n daq --replicas=2
    kubectl scale deployment consumer-telemetry -n daq --replicas=1
    echo "스케일 다운 완료"
    ;;
  set)
    kubectl scale deployment consumer-sensor -n daq --replicas=$COUNT
    echo "consumer-sensor → ${COUNT}개"
    ;;
  status)
    kubectl get pods -n daq
    ;;
  restart)
    kubectl rollout restart deployment/consumer-sensor -n daq
    kubectl rollout restart deployment/consumer-telemetry -n daq
    echo "재시작 완료"
    ;;
  *)
    echo "사용법: ./scale.sh [up|down|set N|status|restart]"
    ;;
esac

kubectl get deployment -n daq
