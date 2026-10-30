"""CPU e memória minuto a minuto de uma execução, para conferir os 2 minutos de aquecimento.

Uso, a partir da raiz do repositório: python3 scripts/piloto/aquecimento.py <pasta da execução>

Para cada contêiner do backend e do Collector, mostra a CPU média e a memória mediana de cada
minuto da carga e o primeiro minuto a partir do qual as duas ficam a menos de 10% do regime,
que é a média dos minutos 3 até o penúltimo. Se esse minuto for 3 ou antes, os 2 minutos
descartados bastam.
"""
import csv
import json
import sys
from pathlib import Path

FAIXA = 0.10


def minutos(arquivo, inicio, fim):
    """{contêiner: [(minuto, cpu %, memória MiB), ...]} entre o início e o fim da carga."""
    por = {}
    with open(arquivo) as f:
        for l in csv.DictReader(f):
            t = float(l["instante"])
            if inicio <= t <= fim:
                m = int((t - inicio) // 60) + 1
                por.setdefault(l["conteiner"], {}).setdefault(m, []).append(
                    (t, float(l["cpu_s"]), float(l["memoria_bytes"])))
    resultado = {}
    for conteiner, ms in por.items():
        linhas = []
        for m in sorted(ms):
            v = ms[m]
            if len(v) < 2 or v[-1][0] <= v[0][0]:
                continue
            cpu = (v[-1][1] - v[0][1]) / (v[-1][0] - v[0][0]) * 100
            memoria = sorted(x[2] for x in v)[len(v) // 2] / 2**20
            linhas.append((m, cpu, memoria))
        resultado[conteiner] = linhas
    return resultado


def estabiliza(linhas):
    """Primeiro minuto a partir do qual CPU e memória ficam na faixa do regime."""
    regime = [l for l in linhas if 3 <= l[0] < linhas[-1][0]]
    if not regime:
        return None
    cpu = sum(l[1] for l in regime) / len(regime)
    memoria = sum(l[2] for l in regime) / len(regime)

    def dentro(l):
        """Se a CPU e a memória do minuto estão a menos de FAIXA do regime."""
        return abs(l[1] - cpu) <= FAIXA * max(cpu, 1) and abs(l[2] - memoria) <= FAIXA * memoria

    for i, l in enumerate(linhas[:-1]):
        if all(dentro(x) for x in linhas[i:-1]):
            return l[0]
    return None


pasta = Path(sys.argv[1])
with open(pasta / "resumo.json") as f:
    resumo = json.load(f)
inicio, fim = resumo["inicio_carga"], resumo["fim_carga"]
for papel, nome in {"backend": "recursos_backend.csv",
                    "collector": "recursos_collector.csv"}.items():
    if not (pasta / nome).exists():
        continue
    for conteiner, linhas in minutos(pasta / nome, inicio, fim).items():
        if not linhas:
            continue
        print(f"{papel} {conteiner}: estabiliza no minuto {estabiliza(linhas)}")
        print("   " + " | ".join(f"{m}: {cpu:.0f}% {mem:.0f} MiB" for m, cpu, mem in linhas))
