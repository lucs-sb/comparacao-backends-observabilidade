"""Geradores de carga, executados como contêineres na rede tcc."""
import subprocess

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


def flog(n):
    _docker("-v", "flog-logs:/var/log/flog", FLOG,
            "-f", "json", "-n", str(n), "-t", "log", "-o", "/var/log/flog/app.log", "-w")