#!/usr/bin/env bash
# Piloto do experimento de consulta (P7): uma repetição nas quatro ferramentas.
# Uso, a partir da raiz do repositório e dentro do tmux: scripts/piloto_consultas.sh
# As execuções já concluídas são puladas, então basta rodar de novo para retomar.
cd "$(dirname "$0")/.." || exit 1
source .venv/bin/activate
mkdir -p resultados
cd orquestrador || exit 1
for f in loki elasticsearch jaeger zipkin; do
  python consultas.py "$f" --experimento piloto-consulta \
    || echo "[$f fixo rep1] falhou, seguindo para a próxima"
done 2>&1 | tee -a ../resultados/piloto-consulta.log
echo "piloto da consulta terminado"
