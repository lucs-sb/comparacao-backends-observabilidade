import requests

from backends.prometheus import Prometheus


class VictoriaMetrics(Prometheus):
    """Aceita a mesma API de consulta do Prometheus, muda só a descarga."""
    pasta = "victoriametrics"
    volume = "vm-data"
    url = "http://localhost:8428"
    saude = "/health"

    def contar(self):
        # A contagem pela consulta esbarra no limite de séries únicas por consulta com
        # 1 milhão de séries. Esta rota devolve o total de séries do banco direto.
        try:
            r = requests.get(f"{self.url}/api/v1/series/count", timeout=300)
            r.raise_for_status()
            return int(r.json()["data"][0])
        except (requests.RequestException, KeyError, IndexError, ValueError):
            return super().contar()  # a consulta do Prometheus, como antes

    def descarregar(self):
        requests.get(f"{self.url}/internal/force_flush", timeout=60).raise_for_status()
