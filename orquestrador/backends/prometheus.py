"""Prometheus: backend de métricas, recebendo por escrita remota (remote write)."""
import requests

from backends.base import Backend


class Prometheus(Backend):
    """Prometheus recebendo amostras por escrita remota, sem coletar alvos."""
    pasta = "prometheus"
    volume = "prom-data"
    url = "http://localhost:9090"
    saude = "/-/ready"

    def consultar(self, promql):
        """Resultado de uma consulta PromQL instantânea."""
        r = requests.get(f"{self.url}/api/v1/query", params={"query": promql}, timeout=300)
        r.raise_for_status()
        return r.json()["data"]["result"]

    def contar(self):
        """Número de séries guardadas.

        Conta todas as séries: nos sintéticos só existem as do avalanche, e no cenário
        realista as métricas da Demo têm outros nomes.
        """
        resultado = self.consultar('count({__name__=~".+"})')
        return int(resultado[0]["value"][1]) if resultado else 0

    def descarregar(self):
        """Nada a fazer, porque o Prometheus grava cada amostra no WAL ao recebê-la."""
        # O Prometheus grava cada amostra no WAL em disco ao recebê-la, então não há o que forçar.
        # O snapshot da API de administração foi descartado porque copia o bloco dentro do
        # próprio volume e dobraria a medida de disco.
        pass
