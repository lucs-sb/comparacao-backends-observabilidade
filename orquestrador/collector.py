"""Collector isolado dos experimentos sintéticos: subida, parada e leitura dos contadores.

O OpenTelemetry Collector roda na instância 2 e recebe a carga dos geradores nos pares de
logs e de traces. Ele é montado a partir de collector/base.yaml, comum a todos os pares, e de
um arquivo do par, como collector/logs-loki.yaml, que define o exportador para o backend.

Os contadores internos do Collector, expostos no formato do Prometheus na porta 8888, dizem
quantos itens ele recebeu e enviou. É com eles que o orquestrador calcula a perda.
"""
import subprocess
import time

import requests

from config import ARQUIVO_VERSOES, HOST_DEMO, RAIZ, arquivos_compose, docker_em

METRICAS = f"http://{HOST_DEMO}:8888/metrics"


def _compose(par, *args):
    """Roda docker compose na pasta collector, com o arquivo do par na variável PAR."""
    pasta = RAIZ / "collector"
    subprocess.run(
        ["docker", "compose", "--env-file", str(ARQUIVO_VERSOES),
         *arquivos_compose(pasta), *args],
        cwd=pasta,
        env={**docker_em(HOST_DEMO), "PAR": par},
        check=True,
        capture_output=True,
    )


def parar():
    """Remove o Collector e o volume dos arquivos de logs lidos por ele.

    O par passado não importa para a remoção, mas o compose precisa de um valor em PAR. O -v
    apaga o volume, para que os arquivos de uma execução de consulta não sejam relidos na
    seguinte.
    """
    _compose("logs-loki", "down", "-v")


def subir(par, espera=60):
    """Recria o Collector com o arquivo do par e espera ele responder.

    Recriar zera os contadores internos, então cada execução começa a contar do zero.
    """
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


def _somar(prefixo, exportador=None):
    """Soma todas as linhas de uma métrica do Collector, com exportador opcional.

    Cada métrica aparece em várias linhas, uma por combinação de rótulos, como receptor,
    exportador e protocolo. O valor fica depois do último espaço da linha. Se o Collector não
    responder, devolve None.
    """
    try:
        texto = requests.get(METRICAS, timeout=5).text
    except requests.RequestException:
        return None
    total = 0.0
    for linha in texto.splitlines():
        if not linha.startswith(prefixo):
            continue
        if exportador and f'exporter="{exportador}"' not in linha:
            continue
        total += float(linha.rsplit(" ", 1)[1])
    return int(total)


def enviados(sinal, exportador=None):
    """Itens que os exportadores entregaram ao backend.

    sinal é log_records, spans ou metric_points. exportador limita a contagem a um
    exportador, quando o Collector tem mais de um, como o da Demo.
    """
    return _somar(f"otelcol_exporter_sent_{sinal}", exportador)


def falhas(sinal, exportador=None):
    """Itens que os exportadores não conseguiram entregar.

    O contador só passa a existir na primeira falha, então a ausência dele significa zero.
    """
    return _somar(f"otelcol_exporter_send_failed_{sinal}", exportador)


def recebidos(sinal):
    """Itens aceitos pelos receptores, antes de qualquer exportador."""
    return _somar(f"otelcol_receiver_accepted_{sinal}")
