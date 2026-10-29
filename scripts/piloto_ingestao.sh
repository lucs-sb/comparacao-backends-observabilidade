#!/usr/bin/env bash
# Piloto da ingestão (P3, P9 e P10): 3 repetições a 75% de S nas seis ferramentas.
# Uso, a partir da raiz do repositório e dentro do tmux: scripts/piloto_ingestao.sh
# Uma execução que falha não interrompe as outras, e as já concluídas são puladas,
# então basta rodar de novo para retomar.
cd "$(dirname "$0")/.." || exit 1
source .venv/bin/activate
mkdir -p resultados
cd orquestrador || exit 1
for rep in 1 2 3; do
  for f in loki elasticsearch jaeger zipkin prometheus victoriametrics; do
    python ingestao.py "$f" --nivel 75 --repeticao "$rep" --experimento piloto-ingestao \
      || echo "[$f 75pct rep$rep] falhou, seguindo para a próxima"
  done
done 2>&1 | tee -a ../resultados/piloto-ingestao.log
echo "piloto da ingestão terminado"
