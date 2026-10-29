#!/usr/bin/env bash
# Piloto da cardinalidade (P8): uma repetição dos três patamares nas duas ferramentas.
# Uso, a partir da raiz do repositório e dentro do tmux: scripts/piloto_cardinalidade.sh
# As execuções já concluídas são puladas, então basta rodar de novo para retomar.
cd "$(dirname "$0")/.." || exit 1
source .venv/bin/activate
mkdir -p resultados
cd orquestrador || exit 1
for series in 100000 500000 1000000; do
  for f in prometheus victoriametrics; do
    python cardinalidade.py "$f" --series "$series" --experimento piloto-cardinalidade \
      || echo "[$f $series séries] falhou, seguindo para a próxima"
  done
done 2>&1 | tee -a ../resultados/piloto-cardinalidade.log
echo "piloto da cardinalidade terminado"
