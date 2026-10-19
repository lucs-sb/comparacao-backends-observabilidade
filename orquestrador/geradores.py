"""Geradores de carga, executados como contêineres na rede tcc."""
import subprocess

from config import HOST_DEMO, VERSOES, docker_em, hosts_extras

TELEMETRYGEN = ("ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:"
                + VERSOES["TELEMETRYGEN_VERSION"])
AVALANCHE = "quay.io/prometheuscommunity/avalanche:" + VERSOES["AVALANCHE_VERSION"]
FLOG = VERSOES["FLOG_IMAGE"]


def _docker(*args, host=None):
    """Sem host, roda na máquina do orquestrador. Com host, roda na instância indicada."""
    subprocess.run(["docker", "run", "--rm", "--network", "tcc", *hosts_extras(), *args],
                   env=docker_em(host) if host else None,
                   check=True, capture_output=True)


def telemetrygen(*args):
    _docker(TELEMETRYGEN, *args, "--otlp-endpoint", "otelcol:4317", "--otlp-insecure")


def avalanche(*args):
    _docker(AVALANCHE, *args)


def flog(n):
    # roda na instância do Collector, porque o arquivo é lido por ele num volume local
    _docker("-v", "flog-logs:/var/log/flog", FLOG,
            "-f", "json", "-n", str(n), "-t", "log", "-o", "/var/log/flog/app.log", "-w",
            host=HOST_DEMO)
