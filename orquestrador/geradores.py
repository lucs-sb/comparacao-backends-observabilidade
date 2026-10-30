"""Imagens dos geradores de carga e execução de geradores curtos.

Os geradores rodam como contêineres na rede Docker tcc, com as versões fixadas em
versoes.env:

- telemetrygen: gera logs e traces no protocolo OTLP e envia ao Collector;
- avalanche: gera métricas e envia direto ao backend por escrita remota (remote write);
- flog: gera linhas de log no formato de servidor web, usadas no experimento de consulta.

As funções deste módulo esperam o gerador terminar e servem para cargas curtas, como as
verificações do ambiente. Os experimentos iniciam os geradores em segundo plano, pelo
módulo carga.
"""
import subprocess

from config import VERSOES, docker_em, hosts_extras

TELEMETRYGEN = ("ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:"
                + VERSOES["TELEMETRYGEN_VERSION"])
AVALANCHE = "quay.io/prometheuscommunity/avalanche:" + VERSOES["AVALANCHE_VERSION"]
FLOG = VERSOES["FLOG_IMAGE"]  # fixada pelo digest, porque a imagem não tem versões marcadas


def _docker(*args, host=None):
    """Roda um contêiner na rede tcc e espera ele terminar.

    Sem host, roda na máquina do orquestrador. Com host, roda na instância indicada.
    """
    subprocess.run(["docker", "run", "--rm", "--network", "tcc", *hosts_extras(), *args],
                   env=docker_em(host) if host else None,
                   check=True, capture_output=True)


def telemetrygen(*args):
    """Roda o telemetrygen enviando ao Collector, com os argumentos dados."""
    _docker(TELEMETRYGEN, *args, "--otlp-endpoint", "otelcol:4317", "--otlp-insecure")


def avalanche(*args):
    """Roda o avalanche com os argumentos dados, que incluem o endereço do backend."""
    _docker(AVALANCHE, *args)
