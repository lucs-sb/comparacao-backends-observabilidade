# consultas/jaeger.py
import csv
import json
from datetime import datetime, timedelta, timezone

from locust import HttpUser, events, task

REPETICOES = 30
URL = "/api/v3/traces"

arquivo = open("/mnt/locust/tempos_jaeger.csv", "w", newline="")
saida = csv.writer(arquivo)
saida.writerow(["inicio", "consulta", "ms", "status", "erro"])


@events.request.add_listener
def registrar(name, response_time, response, exception=None, start_time=None, **kwargs):
    saida.writerow([start_time, name, round(response_time, 3),
                    getattr(response, "status_code", None), exception or ""])


def rfc3339(momento):
    return momento.strftime("%Y-%m-%dT%H:%M:%SZ")


def parametros(**extras):
    agora = datetime.now(timezone.utc)
    return {"query.serviceName": "tcc-teste",
            "query.startTimeMin": rfc3339(agora - timedelta(hours=1)),
            "query.startTimeMax": rfc3339(agora),
            "query.searchDepth": 200, **extras}


class ConsultaJaeger(HttpUser):
    host = "http://jaeger:16686"
    repeticoes = 0

    def consultar(self, nome, params, traces, spans):
        with self.client.get(URL, params=params, name=nome, catch_response=True) as resposta:
            if resposta.status_code != 200:
                resposta.failure(f"status {resposta.status_code}")
                return
            lista = [s for r in resposta.json()["result"]["resourceSpans"]
                     for e in r["scopeSpans"] for s in e["spans"]]
            obtidos = (len({s["traceId"] for s in lista}), len(lista))
            if obtidos != (traces, spans):
                resposta.failure(f"{obtidos[0]} traces e {obtidos[1]} spans, "
                                 f"esperado {traces} e {spans}")

    @task
    def consultas(self):
        self.consultar("busca_servico", parametros(), 100, 600)
        self.consultar("busca_atributo",
                       parametros(**{"query.attributes": json.dumps({"tcc.busca": "alvo"})}),
                       100, 600)
        self.repeticoes += 1
        if self.repeticoes == REPETICOES:
            arquivo.close()
            self.environment.runner.quit()