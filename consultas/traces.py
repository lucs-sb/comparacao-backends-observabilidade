"""Consultas do par de traces no experimento de consulta.

Rodado pelo orquestrador num contêiner do Locust, com um único usuário:
    ALVO=jaeger|zipkin SERVICO=svc-007 REPETICOES=30
    ESPERADO_SERVICO=200,1000 ESPERADO_ATRIBUTO=10,50   (traces e spans)

Três consultas na última hora, com limite de 200 traces: busca_servico, os traces de um
serviço (1% do total); busca_atributo, os traces desse serviço com o atributo tcc.busca=alvo
(1% dos traces dele); e busca_identificador, um trace pelo identificador. O identificador é
obtido uma vez, antes das repetições, pela busca por atributo. Uma resposta 200 com outro
número de traces ou spans conta como falha. Antes das repetições medidas, um aquecimento
repete as três consultas até os tempos estabilizarem (aquecimento.py). Os tempos medidos vão
para /saida/tempos.csv.
"""
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from locust import HttpUser, events, task

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aquecimento import Aquecimento  # noqa: E402

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


aquecimento = Aquecimento()


@events.request.add_listener
def registrar(name, response_time, response, exception=None, start_time=None, **kwargs):
    """Chamada pelo Locust a cada requisição: grava o tempo no aquecimento ou nos medidos."""
    if name == "preparo":
        return  # a busca do identificador não entra nos tempos
    linha = [start_time, name, round(response_time, 3),
             getattr(response, "status_code", None), exception or ""]
    if aquecimento.ativo:
        aquecimento.registrar(*linha)
    else:
        saida.writerow(linha)


def preparar(usuario):
    """Identificador de um dos traces com o atributo, usado em busca_identificador."""
    obtido = usuario.consultar("preparo", usuario.filtro_atributo(), ESPERADO_ATRIBUTO)
    if not obtido or not obtido[2]:
        raise RuntimeError("a busca por atributo não devolveu traces para o identificador")
    return obtido[2][0]


class Base(HttpUser):
    """Usuário do Locust com a conferência das respostas e as três consultas de cada repetição."""
    abstract = True   # o Locust não cria usuários desta classe, só das filhas
    repeticoes = 0
    identificador = None

    def conferir(self, resposta, contar, esperado):
        """Marca a requisição como falha se o status não for 200 ou as contagens forem outras.

        esperado é o par (traces, spans). Devolve o que contar extraiu da resposta, com os
        identificadores dos traces no terceiro item.
        """
        if resposta.status_code != 200:
            resposta.failure(f"status {resposta.status_code}")
            return None
        obtido = contar(resposta.json())
        if obtido[:2] != esperado:
            resposta.failure(f"{obtido[0]} traces e {obtido[1]} spans, esperado {esperado}")
        return obtido

    @task
    def consultas(self):
        """Uma repetição: busca por serviço, por atributo e por identificador, nessa ordem."""
        self.consultar("busca_servico", {}, ESPERADO_SERVICO)
        self.consultar("busca_atributo", self.filtro_atributo(), ESPERADO_ATRIBUTO)
        self.buscar_identificador("busca_identificador", self.identificador)
        if aquecimento.ativo:
            aquecimento.fim_repeticao()
            return
        self.repeticoes += 1
        if self.repeticoes == REPETICOES:
            arquivo.close()
            self.environment.runner.quit()


class Jaeger(Base):
    """Consultas na API v3 do Jaeger, que devolve os spans no formato OTLP."""
    host = "http://jaeger:16686"

    @staticmethod
    def contar(dados):
        """Traces, spans e identificadores dos traces numa resposta da API v3."""
        spans = [s for r in dados.get("result", {}).get("resourceSpans", [])
                 for e in r["scopeSpans"] for s in e["spans"]]
        ids = sorted({s["traceId"] for s in spans})
        return len(ids), len(spans), ids

    def filtro_atributo(self):
        """Parâmetro da busca por atributo, em JSON, o único formato que a API v3 aceita."""
        return {"query.attributes": json.dumps({"tcc.busca": "alvo"})}

    def consultar(self, nome, extras, esperado):
        """Traces do serviço na última hora, com os filtros extras e o limite de traces."""
        agora = datetime.now(timezone.utc)
        params = {"query.serviceName": SERVICO,
                  "query.startTimeMin": (agora - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "query.startTimeMax": agora.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "query.searchDepth": LIMITE, **extras}
        with self.client.get("/api/v3/traces", params=params, name=nome,
                             catch_response=True) as resposta:
            return self.conferir(resposta, self.contar, esperado)

    def buscar_identificador(self, nome, identificador):
        """Um trace pelo identificador, que deve trazer todos os spans dele."""
        with self.client.get(f"/api/v3/traces/{identificador}", name=nome,
                             catch_response=True) as resposta:
            self.conferir(resposta, self.contar, (1, SPANS_POR_TRACE))

    def on_start(self):
        """Antes das repetições, obtém o identificador usado na busca por identificador."""
        self.identificador = preparar(self)


class Zipkin(Base):
    """Consultas na API v2 do Zipkin, que devolve uma lista de traces, cada um uma lista de spans."""
    host = "http://zipkin:9411"

    @staticmethod
    def contar(dados):
        """Traces, spans e identificadores dos traces numa resposta da API v2.

        A busca devolve uma lista de traces, e a busca por identificador, um trace só.
        """
        traces = dados if dados and isinstance(dados[0], list) else [dados]
        ids = sorted({t[0]["traceId"] for t in traces if t})
        return len(ids), sum(len(t) for t in traces), ids

    def filtro_atributo(self):
        """Parâmetro da busca por atributo, no formato chave=valor do Zipkin."""
        return {"annotationQuery": "tcc.busca=alvo"}

    def consultar(self, nome, extras, esperado):
        """Traces do serviço na última hora, com os filtros extras e o limite de traces."""
        params = {"serviceName": SERVICO, "lookback": 3600000, "limit": LIMITE, **extras}
        with self.client.get("/api/v2/traces", params=params, name=nome,
                             catch_response=True) as resposta:
            return self.conferir(resposta, self.contar, esperado)

    def buscar_identificador(self, nome, identificador):
        """Um trace pelo identificador, que deve trazer todos os spans dele."""
        with self.client.get(f"/api/v2/trace/{identificador}", name=nome,
                             catch_response=True) as resposta:
            self.conferir(resposta, self.contar, (1, SPANS_POR_TRACE))

    def on_start(self):
        """Antes das repetições, obtém o identificador usado na busca por identificador."""
        self.identificador = preparar(self)


if ALVO == "jaeger":
    del Zipkin
else:
    del Jaeger
