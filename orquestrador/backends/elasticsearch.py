"""Elasticsearch: backend do par de logs e armazenamento do Jaeger e do Zipkin."""
import time

import requests

from backends.base import Backend
from config import AMBIENTE


class Elasticsearch(Backend):
    """Elasticsearch em nó único, com a memória definida pelo par em que é usado."""
    pasta = "elasticsearch"
    volume = "es-data"
    url = "http://localhost:9200"
    saude = "/_cluster/health?wait_for_status=yellow&timeout=1s"
    indice = "logs-*"   # o exportador do Collector grava os logs em data streams logs-*
    perfil = "logs"     # no par de traces, o Jaeger e o Zipkin trocam para "traces"

    def _variaveis(self):
        """Memória do contêiner e heap da JVM do par, para o compose.

        Os valores vêm do ambiente.env da AWS: ES_MEM_LOGS e ES_HEAP_LOGS no par de logs,
        ES_MEM_TRACES e ES_HEAP_TRACES no de traces. No par de traces, o Elasticsearch divide
        a memória da instância com o Jaeger ou o Zipkin, por isso recebe menos. Sem esses
        valores, o compose usa os do MacBook.
        """
        valores = {}
        for nome in ("ES_MEM", "ES_HEAP"):
            chave = f"{nome}_{self.perfil.upper()}"
            if chave in AMBIENTE:
                valores[nome] = AMBIENTE[chave]
        return valores

    def contar_indice(self, padrao):
        """Número de documentos nos índices que casam com o padrão, como logs-*."""
        r = requests.get(f"{self.url}/{padrao}/_count", timeout=30)
        r.raise_for_status()
        return r.json()["count"]

    def contar(self):
        """Número de logs guardados nos data streams logs-*."""
        return self.contar_indice(self.indice)

    def _gravar(self):
        """Torna todos os documentos contáveis e grava em disco o que estava no translog.

        O refresh torna os documentos visíveis à contagem, e o flush grava o translog nos
        segmentos. Depois de uma saturação, o Elasticsearch pode levar minutos para
        responder, daí o tempo limite longo.
        """
        requests.post(f"{self.url}/_refresh", timeout=600).raise_for_status()
        requests.post(f"{self.url}/_flush?wait_if_ongoing=true", timeout=600).raise_for_status()

    def fusoes_em_andamento(self):
        """Número de fusões de segmentos em andamento, somado entre os nós."""
        r = requests.get(f"{self.url}/_nodes/stats/indices/merge", timeout=30)
        r.raise_for_status()
        return sum(n["indices"]["merges"]["current"] for n in r.json()["nodes"].values())

    def descarregar(self):
        """Grava os dados pendentes, como exigido pela classe base."""
        self._gravar()

    def acomodar(self, espera_fusoes=900):
        """Espera as fusões de segmentos terminarem antes da medida de disco.

        Durante uma fusão, os segmentos antigos e o novo ocupam o disco ao mesmo tempo. Num
        teste com o Jaeger, o mesmo número de spans ocupou de 6,5 a 10,7 GB conforme o
        momento da medida. Com as fusões concluídas, o disco reflete só os dados. A espera é
        limitada a espera_fusoes segundos, e passar disso só gera um aviso.
        """
        limite = time.time() + espera_fusoes
        while self.fusoes_em_andamento() > 0:
            if time.time() > limite:
                print(f"aviso: fusões do Elasticsearch ainda em andamento após {espera_fusoes} s",
                      flush=True)
                break
            time.sleep(10)
        self._gravar()  # libera os arquivos dos segmentos antigos

    def subir(self, espera=180):
        """Sobe o Elasticsearch e ajusta o modelo dos índices de logs.

        Por padrão, os índices de logs criados pelo exportador do Collector teriam uma
        réplica, que nunca é alocada num nó único e deixa o cluster em estado amarelo. O
        modelo logs-otel@custom define 1 shard e 0 réplicas, os mesmos valores usados pelo
        Jaeger e pelo Zipkin, para que os três gravem nas mesmas condições.
        """
        super().subir(espera)
        requests.put(f"{self.url}/_component_template/logs-otel@custom", timeout=30, json={
            "template": {"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
        }).raise_for_status()
