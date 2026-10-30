"""CPU média e memória máxima de cada contêiner em cada degrau de uma rampa.

Uso, a partir da raiz do repositório: python3 scripts/piloto/consumo_degraus.py <pasta da execução>

Mostra o backend sempre, o Collector nos pares de logs e traces e o gerador no par de métricas.
"""
import csv
import sys
from pathlib import Path

ARQUIVOS = {"backend": "recursos_backend.csv", "collector": "recursos_collector.csv",
            "gerador": "recursos_gerador.csv"}
ACOMODACAO = 10  # segundos ignorados no início e no fim de cada degrau, como no orquestrador


def consumo(arquivo, inicio, fim):
    por = {}
    with open(arquivo) as f:
        for l in csv.DictReader(f):
            t = float(l["instante"])
            if inicio <= t <= fim:
                por.setdefault(l["conteiner"], []).append(
                    (t, float(l["cpu_s"]), float(l["memoria_bytes"])))
    return {c: f"CPU {round((v[-1][1] - v[0][1]) / (v[-1][0] - v[0][0]) * 100)}%, "
               f"mem {round(max(x[2] for x in v) / 2**20)} MiB"
            for c, v in por.items() if len(v) > 1}


pasta = Path(sys.argv[1])
with open(pasta / "degraus.csv") as f:
    degraus = list(csv.DictReader(f))
for d in degraus:
    inicio, fim = float(d["inicio"]) + ACOMODACAO, float(d["fim"]) - ACOMODACAO
    print(d["degrau"], d["alvo"], d["aceita"], d["estado"])
    for papel, nome in ARQUIVOS.items():
        if (pasta / nome).exists():
            print(f"   {papel + ':':<10}", consumo(pasta / nome, inicio, fim))
