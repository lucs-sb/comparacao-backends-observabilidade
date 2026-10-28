import requests

from backends.base import Backend


class Loki(Backend):
    pasta = "loki"
    volume = "loki-data"
    url = "http://localhost:3100"
    saude = "/ready"

    def contar(self):
        consulta = 'sum(count_over_time({service_name=~".+"}[1h]))'
        r = requests.get(f"{self.url}/loki/api/v1/query", params={"query": consulta}, timeout=600)
        r.raise_for_status()
        resultado = r.json()["data"]["result"]
        return int(resultado[0]["value"][1]) if resultado else 0

    def descarregar(self):
        requests.post(f"{self.url}/flush", timeout=600).raise_for_status()
