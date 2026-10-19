"""Collector isolado: subida por par e leitura das métricas internas."""
import os
import subprocess
import time

import requests

from config import ARQUIVO_VERSOES, RAIZ

METRICAS = "http://localhost:8888/metrics"


def _compose(par, *args):
    subprocess.run(
        ["docker", "compose", "--env-file", str(ARQUIVO_VERSOES), *args],
        cwd=RAIZ / "collector",
        env={**os.environ, "PAR": par},
        check=True,
        capture_output=True,
    )


def parar():
    # -v apaga também o volume do flog, para que arquivos de outra execução não sejam relidos
    _compose("logs-loki", "down", "-v")


def subir(par, espera=60):
    """Recria o Collector, o que zera as métricas internas."""
    parar()
    _compose(par, "up", "-d")
    limite = time.time() + espera
    while time.time() < limite:
        try:
            if requests.get(METRICAS, timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise TimeoutError(f"Collector não respondeu em {espera} s")


def _somar(prefixo):
    try:
        texto = requests.get(METRICAS, timeout=5).text
    except requests.RequestException:
        return None
    total = 0.0
    for linha in texto.splitlines():
        if linha.startswith(prefixo):
            total += float(linha.rsplit(" ", 1)[1])
    return int(total)


def enviados(sinal):
    """sinal: log_records, spans ou metric_points."""
    return _somar(f"otelcol_exporter_sent_{sinal}")


def falhas(sinal):
    # o contador só passa a existir na primeira falha, então ausência significa zero
    return _somar(f"otelcol_exporter_send_failed_{sinal}")
