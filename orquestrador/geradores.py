"""Geradores de carga, executados como contêineres na rede tcc."""
import subprocess
import time

from config import VERSOES

TELEMETRYGEN = ("ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:"
                + VERSOES["TELEMETRYGEN_VERSION"])
AVALANCHE = "quay.io/prometheuscommunity/avalanche:" + VERSOES["AVALANCHE_VERSION"]
FLOG = VERSOES["FLOG_IMAGE"]


def _docker(*args):
    subprocess.run(["docker", "run", "--rm", "--network", "tcc", *args],
                   check=True, capture_output=True)


def telemetrygen(*args):
    _docker(TELEMETRYGEN, *args, "--otlp-endpoint", "otelcol:4317", "--otlp-insecure")


def avalanche(*args):
    _docker(AVALANCHE, *args)


def avalanche_alvo(*args, duracao=300, porta=9001):
    """Avalanche como alvo de coleta do receptor Prometheus do Collector."""
    subprocess.run(["docker", "rm", "-f", "avalanche"], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", "avalanche", "--network", "tcc",
                    AVALANCHE, *args, f"--port={porta}"], check=True, capture_output=True)
    time.sleep(duracao)
    subprocess.run(["docker", "rm", "-f", "avalanche"], check=True, capture_output=True)


def flog(n):
    _docker("-v", "flog-logs:/var/log/flog", FLOG,
            "-f", "json", "-n", str(n), "-t", "log", "-o", "/var/log/flog/app.log", "-w")
