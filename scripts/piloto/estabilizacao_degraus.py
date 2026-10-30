"""Evolução de cada degrau minuto a minuto, para conferir a duração dos degraus (P2).

Uso, a partir da raiz do repositório: python3 scripts/piloto/estabilizacao_degraus.py <pasta da execução>

Para cada degrau, mostra por minuto a taxa entregue ao backend e, nos pares com Collector, a
fila média do exportador. O degrau é estável quando o segundo e o terceiro minuto entregam
taxas a menos de 5% uma da outra e a fila não cresce mais de 10% da capacidade depois do
primeiro minuto.
"""
import csv
import sys
from pathlib import Path

TOLERANCIA = 0.05


def por_minuto(linhas, inicio, chave):
    """Taxa de uma coluna acumulada e leituras de cada minuto do degrau."""
    minutos = {}
    for l in linhas:
        minutos.setdefault(int((float(l["instante"]) - inicio) // 60), []).append(l)
    resultado = []
    for m in sorted(minutos):
        ls = minutos[m]
        t0, t1 = float(ls[0]["instante"]), float(ls[-1]["instante"])
        taxa = (float(ls[-1][chave]) - float(ls[0][chave])) / (t1 - t0) if t1 > t0 else None
        resultado.append((m + 1, taxa, ls))
    return resultado


pasta = Path(sys.argv[1])
with open(pasta / "degraus.csv") as f:
    degraus = list(csv.DictReader(f))
with open(pasta / "amostras.csv") as f:
    amostras = list(csv.DictReader(f))
com_collector = "enviados" in amostras[0]
chave = "enviados" if com_collector else "gravadas"

for d in degraus:
    linhas = [a for a in amostras if a["degrau"] == d["degrau"]]
    minutos = por_minuto(linhas, float(d["inicio"]), chave)
    partes = []
    for m, taxa, ls in minutos:
        texto = f"min {m}: {round(taxa) if taxa else '-'}/s"
        if com_collector:
            fila = sum(float(l["fila"]) for l in ls) / len(ls)
            texto += f", fila {fila:.1f}"
        partes.append(texto)
    taxas = [t for _, t, _ in minutos if t]
    estavel = "-"
    if len(taxas) >= 3:
        variacao = abs(taxas[-1] - taxas[-2]) / taxas[-2]
        filas_crescem = False
        if com_collector:
            medias = [sum(float(l["fila"]) for l in ls) / len(ls) for _, _, ls in minutos]
            # crescimento acima de 10% da capacidade, para ignorar oscilações de um ou dois lotes
            capacidade = float(linhas[-1]["capacidade_fila"]) or 1.0
            filas_crescem = medias[-1] > medias[1] + 0.1 * capacidade
        estavel = "estável" if variacao <= TOLERANCIA and not filas_crescem else "instável"
    print(f"degrau {d['degrau']} alvo {d['alvo']} {d['estado']}: " + " | ".join(partes)
          + f" -> {estavel}")
