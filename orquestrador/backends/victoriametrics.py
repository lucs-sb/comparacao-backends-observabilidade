"""VictoriaMetrics: backend de métricas compatível com a API de consulta do Prometheus."""
import requests

from backends.prometheus import Prometheus


class VictoriaMetrics(Prometheus):
    """Herda do Prometheus a consulta, e muda a contagem e a descarga."""
    pasta = "victoriametrics"
    volume = "vm-data"
    url = "http://localhost:8428"
    saude = "/health"

    def contar(self):
        """Número de séries guardadas, pela rota própria do VictoriaMetrics."""
        # A contagem pela consulta esbarra no limite de séries únicas por consulta com
        # 1 milhão de séries. Esta rota devolve o total de séries do banco direto.
        try:
            r = requests.get(f"{self.url}/api/v1/series/count", timeout=300)
            r.raise_for_status()
            return int(r.json()["data"][0])
        except (requests.RequestException, KeyError, IndexError, ValueError):
            return super().contar()  # se a rota falhar, conta pela consulta do Prometheus

    def descarregar(self):
        """Grava em disco as amostras que ainda estão no buffer em memória."""
        requests.get(f"{self.url}/internal/force_flush", timeout=60).raise_for_status()
