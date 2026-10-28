import requests

from backends.base import Backend
from config import AMBIENTE


class Elasticsearch(Backend):
    pasta = "elasticsearch"
    volume = "es-data"
    url = "http://localhost:9200"
    saude = "/_cluster/health?wait_for_status=yellow&timeout=1s"
    indice = "logs-*"   # o exportador do Collector grava os logs em data streams logs-*
    perfil = "logs"     # no par de traces, o Jaeger e o Zipkin trocam para "traces" (L9)

    def _variaveis(self):
        # Memória do contêiner e heap por par, definidos no ambiente.env da AWS (L9):
        # ES_MEM_LOGS e ES_HEAP_LOGS, ES_MEM_TRACES e ES_HEAP_TRACES.
        # Sem eles, o compose usa os valores do MacBook.
        valores = {}
        for nome in ("ES_MEM", "ES_HEAP"):
            chave = f"{nome}_{self.perfil.upper()}"
            if chave in AMBIENTE:
                valores[nome] = AMBIENTE[chave]
        return valores

    def contar_indice(self, padrao):
        r = requests.get(f"{self.url}/{padrao}/_count", timeout=30)
        r.raise_for_status()
        return r.json()["count"]

    def contar(self):
        return self.contar_indice(self.indice)

    def descarregar(self):
        # refresh torna os documentos visíveis à contagem, flush grava o translog nos segmentos.
        # Depois de uma saturação, o Elasticsearch pode levar minutos para responder (A20).
        requests.post(f"{self.url}/_refresh", timeout=600).raise_for_status()
        requests.post(f"{self.url}/_flush?wait_if_ongoing=true", timeout=600).raise_for_status()

    def subir(self, espera=180):
        super().subir(espera)
        # 1 shard e 0 réplicas nos data streams de logs, como no Jaeger e no Zipkin (A6)
        requests.put(f"{self.url}/_component_template/logs-otel@custom", timeout=30, json={
            "template": {"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
        }).raise_for_status()
