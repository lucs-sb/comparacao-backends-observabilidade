import requests

from backends.prometheus import Prometheus


class VictoriaMetrics(Prometheus):
    """Aceita a mesma API de consulta do Prometheus, muda só a descarga."""
    pasta = "victoriametrics"
    volume = "vm-data"
    url = "http://localhost:8428"
    saude = "/health"

    def descarregar(self):
        requests.get(f"{self.url}/internal/force_flush", timeout=60).raise_for_status()
