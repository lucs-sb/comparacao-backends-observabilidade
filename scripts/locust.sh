#!/usr/bin/env bash
# uso, a partir da raiz: scripts/locust.sh <arquivo em consultas/> [alvo das métricas]
# exemplos: scripts/locust.sh loki.py      scripts/locust.sh metricas.py prometheus
set -euo pipefail
source versoes.env
HOSTS=()
if [ -f ambiente.env ]; then
  source ambiente.env
  for nome in loki elasticsearch jaeger zipkin prometheus victoriametrics; do
    HOSTS+=(--add-host "$nome:$IP_BACKEND")
  done
fi
docker run --rm --network tcc ${HOSTS[@]+"${HOSTS[@]}"} -e ALVO="${2:-}" \
  -v "$PWD/consultas:/mnt/locust" "locustio/locust:$LOCUST_VERSION" \
  -f "/mnt/locust/$1" --headless -u 1 -r 1 --run-time 5m
