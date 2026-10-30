"""Consultas do par de traces no experimento de consulta (seção 4.7, P7).

Rodado pelo orquestrador num contêiner do Locust, com um único usuário:
    ALVO=jaeger|zipkin SERVICO=svc-007 REPETICOES=30
    ESPERADO_SERVICO=200,1000 ESPERADO_ATRIBUTO=10,50   (traces e spans)

Três consultas na última hora, com limite de 200 traces: busca_servico, os traces de um
serviço (1% do total); busca_atributo, os traces desse serviço com o atributo tcc.busca=alvo
(1% dos traces dele); e busca_identificador, um trace pelo identificador. O identificador é
obtido uma vez, antes das repetições, pela busca por atributo. Uma resposta 200 com outro
número de traces ou spans conta como falha. Os tempos vão para /saida/tempos.csv.
"""
import csv
import json
import os
from datetime import datetime, timedelta, timezone

from locust import HttpUser, events, task

ALVO = os.environ["ALVO"]
SERVICO = os.environ["SERVICO"]
REPETICOES = int(os.environ.get("REPETICOES", "30"))
ESPERADO_SERVICO = tuple(int(x) for x in os.environ["ESPERADO_SERVICO"].split(","))
ESPERADO_ATRIBUTO = tuple(int(x) for x in os.environ["ESPERADO_ATRIBUTO"].split(","))
SPANS_POR_TRACE = ESPERADO_ATRIBUTO[1] // ESPERADO_ATRIBUTO[0]
LIMITE = 200

arquivo = open("/saida/tempos.csv", "w", newline="")
saida = csv.writer(arquivo)
saida.writerow(["inicio", "consulta", "ms", "status", "erro"])


@events.request.add_listener
def registrar(name, response_time, response, exception=None, start_time=None, **kwargs):
    if name == "preparo":
        return  # a busca do identificador não entra nos tempos
    saida.writerow([start_time, name, round(response_time, 3),
                    getattr(response, "status_code", None), exception or ""])


def preparar(usuario):
    """Identificador de um dos traces com o atributo, usado em busca_identificador."""
    obtido = usuario.consultar("preparo", usuario.filtro_atributo(), ESPERADO_ATRIBUTO)
    if not obtido or not obtido[2]:
        raise RuntimeError("a busca por atributo não devolveu traces para o identificador")
    return obtido[2][0]


class Base(HttpUser):
    abstract = True
    repeticoes = 0
    identificador = None

    def conferir(self, resposta, contar, esperado):
        if resposta.status_code != 200:
            resposta.failure(f"status {resposta.status_code}")
            return None
        obtido = contar(resposta.json())
        if obtido[:2] != esperado:
            resposta.failure(f"{obtido[0]} traces e {obtido[1]} spans, esperado {esperado}")
        return obtido

    @task
    def consultas(self):
        self.consultar("busca_servico", {}, ESPERADO_SERVICO)
        self.consultar("busca_atributo", self.filtro_atributo(), ESPERADO_ATRIBUTO)
        self.buscar_identificador("busca_identificador", self.identificador)
        self.repeticoes += 1
        if self.repeticoes == REPETICOES:
            arquivo.close()
            self.environment.runner.quit()


class Jaeger(Base):
    host = "http://jaeger:16686"

    @staticmethod
    def contar(dados):
        spans = [s for r in dados.get("result", {}).get("resourceSpans", [])
                 for e in r["scopeSpans"] for s in e["spans"]]
        ids = sorted({s["traceId"] for s in spans})
        return len(ids), len(spans), ids

    def filtro_atributo(self):
        return {"query.attributes": json.dumps({"tcc.busca": "alvo"})}

    def consultar(self, nome, extras, esperado):
        agora = datetime.now(timezone.utc)
        params = {"query.serviceName": SERVICO,
                  "query.startTimeMin": (agora - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "query.startTimeMax": agora.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "query.searchDepth": LIMITE, **extras}
        with self.client.get("/api/v3/traces", params=params, name=nome,
                             catch_response=True) as resposta:
            return self.conferir(resposta, self.contar, esperado)

    def buscar_identificador(self, nome, identificador):
        with self.client.get(f"/api/v3/traces/{identificador}", name=nome,
                             catch_response=True) as resposta:
            self.conferir(resposta, self.contar, (1, SPANS_POR_TRACE))

    def on_start(self):
        self.identificador = preparar(self)


class Zipkin(Base):
    host = "http://zipkin:9411"

    @staticmethod
    def contar(dados):
        traces = dados if dados and isinstance(dados[0], list) else [dados]
        ids = sorted({t[0]["traceId"] for t in traces if t})
        return len(ids), sum(len(t) for t in traces), ids

    def filtro_atributo(self):
        return {"annotationQuery": "tcc.busca=alvo"}

    def consultar(self, nome, extras, esperado):
        params = {"serviceName": SERVICO, "lookback": 3600000, "limit": LIMITE, **extras}
        with self.client.get("/api/v2/traces", params=params, name=nome,
                             catch_response=True) as resposta:
            return self.conferir(resposta, self.contar, esperado)

    def buscar_identificador(self, nome, identificador):
        with self.client.get(f"/api/v2/trace/{identificador}", name=nome,
                             catch_response=True) as resposta:
            self.conferir(resposta, self.contar, (1, SPANS_POR_TRACE))

    def on_start(self):
        self.identificador = preparar(self)


if ALVO == "jaeger":
    del Zipkin
else:
    del Jaeger
