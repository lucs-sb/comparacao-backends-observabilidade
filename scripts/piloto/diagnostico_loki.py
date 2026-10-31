"""Diagnóstico do Loki: tempo das consultas antes e depois da descarga dos blocos em memória.

Na repetição 2 do experimento de consulta, a busca textual do Loki caiu de cerca de 320 ms
para 148 ms de uma consulta para a seguinte, cerca de 25 segundos depois da descarga. Este
script mede as duas consultas a cada 2 segundos, junto com o número de blocos de dados
(chunks) na memória do ingester, em duas fases:

1. sem descarga, por 2 minutos, com os dados só na memória do ingester;
2. depois do POST /flush, por 3 minutos, enquanto os blocos são gravados no disco e
   liberados da memória.

Assim fica visível em que estado cada tempo foi medido e quanto tempo a transição leva.

Uso, na instância 3, a partir da raiz do repositório, com o rodar.py parado:
    .venv/bin/python scripts/piloto/diagnostico_loki.py
Grava resultados/diagnostico-loki.csv.
"""
import csv
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "orquestrador"))
import collector   # noqa: E402
from backends.loki import Loki   # noqa: E402
from config import RAIZ   # noqa: E402
from consultas import LIMITE_LOGS, LINHAS, SERVICO, TERMO, copiar_logs, garantir_logs   # noqa: E402
from execucao import TODOS   # noqa: E402

CONSULTAS = {"filtro_rotulo": f'{{service_name="{SERVICO}"}}',
             "busca_textual": f'{{service_name=~"svc-.+"}} |= "{TERMO}"'}
METRICAS = ("loki_ingester_memory_chunks", "loki_ingester_flush_queue_length")


def consultar(loki, logql):
    """Tempo da consulta em milissegundos e número de linhas devolvidas, como no Locust."""
    fim = time.time_ns()
    params = {"query": logql, "limit": LIMITE_LOGS, "direction": "backward",
              "start": fim - 3600 * 10**9, "end": fim}
    inicio = time.perf_counter()
    r = requests.get(f"{loki.url}/loki/api/v1/query_range", params=params,
                     headers={"Cache-Control": "no-cache"}, timeout=60)
    ms = (time.perf_counter() - inicio) * 1000
    linhas = sum(len(s["values"]) for s in r.json()["data"]["result"])
    return round(ms, 1), linhas


def metricas(loki):
    """Blocos na memória do ingester e tamanho da fila de descarga."""
    valores = dict.fromkeys(METRICAS, 0.0)
    for linha in requests.get(f"{loki.url}/metrics", timeout=10).text.splitlines():
        for nome in METRICAS:
            if linha.startswith(nome):
                valores[nome] += float(linha.rsplit(" ", 1)[1])
    return valores


def medir(loki, fase, duracao, saida, t0):
    """Mede as consultas e as métricas a cada 2 segundos durante a fase."""
    fim = time.time() + duracao
    while time.time() < fim:
        m = metricas(loki)
        linha = {"fase": fase, "segundos": round(time.time() - t0),
                 "chunks_em_memoria": int(m["loki_ingester_memory_chunks"]),
                 "fila_descarga": int(m["loki_ingester_flush_queue_length"])}
        for nome, logql in CONSULTAS.items():
            linha[f"{nome}_ms"], linha[f"{nome}_linhas"] = consultar(loki, logql)
        saida.writerow(linha)
        print(f"{linha['fase']:<16} {linha['segundos']:>4} s  chunks {linha['chunks_em_memoria']:>6}  "
              f"fila {linha['fila_descarga']:>4}  filtro {linha['filtro_rotulo_ms']:>7} ms  "
              f"busca {linha['busca_textual_ms']:>7} ms", flush=True)
        time.sleep(2)


def main():
    """Prepara o Loki com o volume fixo da consulta e mede as duas fases."""
    loki = Loki()
    print("limpando o ambiente e subindo o Loki e o Collector", flush=True)
    collector.parar()
    for classe in TODOS:
        classe().limpar()
    loki.subir()
    collector.subir("consulta-loki")
    garantir_logs()
    copiar_logs()
    while loki.contar() < LINHAS:
        time.sleep(2)
    print("1 milhão de linhas guardadas, medindo", flush=True)
    collector.parar()   # nada mais chega ao Loki durante a medição
    arquivo = RAIZ / "resultados" / "diagnostico-loki.csv"
    campos = ["fase", "segundos", "chunks_em_memoria", "fila_descarga", "filtro_rotulo_ms",
              "filtro_rotulo_linhas", "busca_textual_ms", "busca_textual_linhas"]
    with open(arquivo, "w", newline="") as f:
        saida = csv.DictWriter(f, fieldnames=campos)
        saida.writeheader()
        t0 = time.time()
        medir(loki, "sem descarga", 120, saida, t0)
        print("descarregando (POST /flush)", flush=True)
        loki.descarregar()
        medir(loki, "depois da descarga", 180, saida, t0)
    print(f"resultados em {arquivo}")


if __name__ == "__main__":
    main()
