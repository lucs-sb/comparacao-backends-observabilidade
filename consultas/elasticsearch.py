# consultas/elasticsearch.py
import csv

from locust import HttpUser, events, task

REPETICOES = 30
URL = "/logs-*/_search?request_cache=false"
SERVICO = {"term": {"resource.attributes.service.name": "tcc-teste"}}
JANELA = {"range": {"@timestamp": {"gte": "now-1h"}}}

arquivo = open("/mnt/locust/tempos_elasticsearch.csv", "w", newline="")
saida = csv.writer(arquivo)
saida.writerow(["inicio", "consulta", "ms", "status", "erro"])


@events.request.add_listener
def registrar(name, response_time, response, exception=None, start_time=None, **kwargs):
    saida.writerow([start_time, name, round(response_time, 3),
                    getattr(response, "status_code", None), exception or ""])


def corpo(*condicoes):
    return {"size": 5000, "sort": [{"@timestamp": "desc"}],
            "query": {"bool": {"filter": [SERVICO, JANELA], "must": list(condicoes)}}}


class ConsultaElasticsearch(HttpUser):
    host = "http://elasticsearch:9200"
    repeticoes = 0

    def consultar(self, nome, consulta, esperado):
        with self.client.post(URL, json=consulta, name=nome, catch_response=True) as resposta:
            if resposta.status_code != 200:
                resposta.failure(f"status {resposta.status_code}")
                return
            linhas = len(resposta.json()["hits"]["hits"])
            if linhas != esperado:
                resposta.failure(f"{linhas} linhas, esperado {esperado}")

    @task
    def consultas(self):
        self.consultar("filtro_rotulo", corpo(), 1000)
        self.consultar("busca_textual", corpo({"match": {"body.text": "teste"}}), 1000)
        self.repeticoes += 1
        if self.repeticoes == REPETICOES:
            arquivo.close()
            self.environment.runner.quit()