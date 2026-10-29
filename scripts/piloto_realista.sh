#!/usr/bin/env bash
# Piloto do cenário realista: 2 repetições com 50 usuários nas seis ferramentas (P4 e A27).
# Confere se o disco por item fica estável entre repetições com o volume da Demo.
# Uso, a partir da raiz do repositório e dentro do tmux: scripts/piloto_realista.sh
# As execuções já concluídas são puladas, então basta rodar de novo para retomar.
cd "$(dirname "$0")/.." || exit 1
source .venv/bin/activate
mkdir -p resultados
cd orquestrador || exit 1
for rep in 1 2; do
  for f in loki elasticsearch jaeger zipkin prometheus victoriametrics; do
    python realista.py execucao "$f" --usuarios 50 --repeticao "$rep" --experimento piloto-realista \
      || echo "[$f 50usuarios rep$rep] falhou, seguindo para a próxima"
  done
done 2>&1 | tee -a ../resultados/piloto-realista.log
echo "piloto do cenário realista terminado"
