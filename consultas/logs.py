"""Consultas do par de logs no experimento de consulta (seção 4.7, P7).

Rodado pelo orquestrador num contêiner do Locust, com um único usuário:
    ALVO=loki|elasticsearch SERVICO=svc-007 TERMO=marcadortcc ESPERADO=1000 REPETICOES=30

Duas consultas, sempre na última hora, com os 1000 registros mais recentes primeiro e sem
cache de resultados: filtro_rotulo, que pega os logs de um serviço (1% do total), e
busca_textual, que procura um termo presente em 1% das linhas de todos os serviços. A busca
textual do Loki procura um trecho em qualquer posição, e a do Elasticsearch, a palavra inteira,
então o termo é sempre uma palavra inteira. Uma resposta 200 com outro número de registros
conta como falha. Os tempos vão para /saida/tempos.csv.
"""
import csv
import os
import time

from locust import HttpUser, events, task

ALVO = os.environ["ALVO"]
SERVICO = os.environ["SERVICO"]
TERMO = os.environ["TERMO"]
ESPERADO = int(os.environ["ESPERADO"])
REPETICOES = int(os.environ.get("REPETICOES", "30"))
LIMITE = 1000   # linhas por consulta, o padrão do Grafana para logs do Loki, usado nos dois

arquivo = open("/saida/tempos.csv", "w", newline="")
saida = csv.writer(arquivo)
saida.writerow(["inicio", "consulta", "ms", "status", "erro"])


@events.request.add_listener
def registrar(name, response_time, response, exception=None, start_time=None, **kwargs):
    saida.writerow([start_time, name, round(response_time, 3),
                    getattr(response, "status_code", None), exception or ""])


class Base(HttpUser):
    abstract = True
    repeticoes = 0

    def conferir(self, resposta, contar):
        if resposta.status_code != 200:
            resposta.failure(f"status {resposta.status_code}")
            return
        obtido = contar(resposta.json())
        if obtido != ESPERADO:
            resposta.failure(f"{obtido} registros, esperado {ESPERADO}")

    def terminar(self):
        self.repeticoes += 1
        if self.repeticoes == REPETICOES:
            arquivo.close()
            self.environment.runner.quit()


class Loki(Base):
    host = "http://loki:3100"

    def consultar(self, nome, logql):
        fim = time.time_ns()
        params = {"query": logql, "limit": LIMITE, "direction": "backward",
                  "start": fim - 3600 * 10**9, "end": fim}
        with self.client.get("/loki/api/v1/query_range", params=params, name=nome,
                             headers={"Cache-Control": "no-cache"},
                             catch_response=True) as resposta:
            self.conferir(resposta, lambda d: sum(len(s["values"]) for s in d["data"]["result"]))

    @task
    def consultas(self):
        self.consultar("filtro_rotulo", f'{{service_name="{SERVICO}"}}')
        self.consultar("busca_textual", f'{{service_name=~"svc-.+"}} |= "{TERMO}"')
        self.terminar()


class Elasticsearch(Base):
    host = "http://elasticsearch:9200"

    def consultar(self, nome, *condicoes, filtros=()):
        corpo = {"size": LIMITE, "sort": [{"@timestamp": "desc"}], "track_total_hits": False,
                 "query": {"bool": {
                     "filter": [{"range": {"@timestamp": {"gte": "now-1h"}}}, *filtros],
                     "must": list(condicoes)}}}
        with self.client.post("/logs-*/_search?request_cache=false", json=corpo, name=nome,
                              catch_response=True) as resposta:
            self.conferir(resposta, lambda d: len(d["hits"]["hits"]))

    @task
    def consultas(self):
        self.consultar("filtro_rotulo",
                       filtros=[{"term": {"resource.attributes.service.name": SERVICO}}])
        self.consultar("busca_textual", {"match": {"body.text": TERMO}})
        self.terminar()


# o Locust usa todas as classes de usuário do arquivo, então só a do alvo fica visível
if ALVO == "loki":
    del Elasticsearch
else:
    del Loki
