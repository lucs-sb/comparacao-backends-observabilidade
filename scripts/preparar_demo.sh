#!/usr/bin/env bash
# uso: PAR=<nome do par> scripts/preparar_demo.sh
set -euo pipefail
source versoes.env
DESTINO=demo/opentelemetry-demo
[ -d "$DESTINO" ] || git clone --depth 1 --branch "$DEMO_TAG" \
  https://github.com/open-telemetry/opentelemetry-demo.git "$DESTINO"
if [ -n "${PAR:-}" ]; then
  cp "demo/extras-${PAR}.yml" "$DESTINO/src/otel-collector/otelcol-config-extras.yml"
fi