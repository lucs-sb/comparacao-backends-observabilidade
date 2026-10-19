import requests

from backends.base import Backend


class Prometheus(Backend):
    pasta = "prometheus"
    volume = "prom-data"
    url = "http://localhost:9090"
    saude = "/-/ready"
    prefixo = "avalanche"

    def consultar(self, promql):
        r = requests.get(f"{self.url}/api/v1/query", params={"query": promql}, timeout=30)
        r.raise_for_status()
        return r.json()["data"]["result"]

    def contar(self):
        resultado = self.consultar(f'count({{__name__=~"{self.prefixo}.*"}})')
        return int(resultado[0]["value"][1]) if resultado else 0

    def nomes(self):
        r = requests.get(f"{self.url}/api/v1/label/__name__/values", timeout=30)
        r.raise_for_status()
        return sorted(n for n in r.json()["data"] if n.startswith(self.prefixo))

    def descarregar(self):
        # O Prometheus grava cada amostra no WAL em disco ao recebê-la, então não há o que forçar.
        # O snapshot da API de administração foi descartado porque copia o bloco dentro do
        # próprio volume e dobraria a medida de disco.
        pass
