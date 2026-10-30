"""Variação entre repetições, para decidir quantas repetições cada condição precisa.

Uso, a partir da raiz do repositório: python3 scripts/piloto/variacao.py <pasta do experimento>
Exemplo: python3 scripts/piloto/variacao.py resultados/piloto-ingestao

Para cada ferramenta e condição, mostra média, desvio padrão e coeficiente de variação (CV)
da CPU e da memória do backend e do disco por item. Depois compara as duas ferramentas de
cada par: se a diferença das médias for menor que cerca de 3 desvios padrão, 5 repetições
podem não bastar para separar as duas.
"""
import json
import statistics
import sys
from pathlib import Path

METRICAS = {
    "cpu_pct": lambda r: sum(v["cpu_pct"] or 0 for v in r["recursos_backend"].values()),
    "memoria_mib": lambda r: sum(v["memoria_mediana_mib"] for v in r["recursos_backend"].values()),
    "bytes_por_item": lambda r: r.get("bytes_por_item"),
}


def carregar(raiz):
    """Resumos agrupados por par, condição e ferramenta."""
    grupos = {}
    for arquivo in sorted(Path(raiz).glob("*/*/*/rep*/resumo.json")):
        with open(arquivo) as f:
            r = json.load(f)
        e = r["execucao"]
        grupos.setdefault((e["par"], e["condicao"], e["ferramenta"]), []).append(r)
    return grupos


def estatisticas(valores):
    """Média, desvio padrão, coeficiente de variação em porcentagem e número de valores."""
    valores = [v for v in valores if v is not None]
    if len(valores) < 2:
        return None
    media = statistics.mean(valores)
    desvio = statistics.stdev(valores)
    return media, desvio, (100 * desvio / media if media else None), len(valores)


grupos = carregar(sys.argv[1])
resumos = {}
for (par, condicao, ferramenta), rs in sorted(grupos.items()):
    print(f"{par} {condicao} {ferramenta} ({len(rs)} repetições)")
    for nome, extrair in METRICAS.items():
        e = estatisticas([extrair(r) for r in rs])
        resumos[(par, condicao, ferramenta, nome)] = e
        if e:
            media, desvio, cv, _ = e
            print(f"   {nome}: média {media:.1f}, desvio {desvio:.1f}, CV {cv:.1f}%")

print("\nComparação entre as ferramentas de cada par")
pares = {}
for (par, condicao, ferramenta) in grupos:
    pares.setdefault((par, condicao), []).append(ferramenta)
for (par, condicao), ferramentas in sorted(pares.items()):
    if len(ferramentas) != 2:
        continue
    a, b = sorted(ferramentas)
    for nome in METRICAS:
        ea, eb = resumos.get((par, condicao, a, nome)), resumos.get((par, condicao, b, nome))
        if not ea or not eb:
            continue
        diferenca = abs(ea[0] - eb[0])
        desvio = max(ea[1], eb[1]) or 1e-9
        veredito = "separa bem" if diferenca >= 3 * desvio else "perto do limite"
        print(f"{par} {condicao} {nome}: {a} {ea[0]:.1f} contra {b} {eb[0]:.1f}, "
              f"diferença de {diferenca / desvio:.1f} desvios: {veredito}")
