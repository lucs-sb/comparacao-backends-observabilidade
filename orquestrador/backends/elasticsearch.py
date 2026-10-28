import requests

from backends.base import Backend


class Elasticsearch(Backend):
    pasta = "elasticsearch"
    volume = "es-data"
    url = "http://localhost:9200"
    saude = "/_cluster/health?wait_for_status=yellow&timeout=1s"
    indice = "logs-*"   # o exportador do Collector grava os logs em data streams logs-*

    def contar_indice(self, padrao):
        r = requests.get(f"{self.url}/{padrao}/_count", timeout=30)
        r.raise_for_status()
        return r.json()["count"]

    def contar(self):
        return self.contar_indice(self.indice)

    def descarregar(self):
        # refresh torna os documentos visíveis à contagem, flush grava o translog nos segmentos
        requests.post(f"{self.url}/_refresh", timeout=600).raise_for_status()
        requests.post(f"{self.url}/_flush?wait_if_ongoing=true", timeout=600).raise_for_status()

    def subir(self, espera=180):
        super().subir(espera)
        requests.put(f"{self.url}/_component_template/logs-otel@custom", timeout=30, json={
            "template": {"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
        }).raise_for_status()
