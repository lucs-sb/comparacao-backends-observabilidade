# consultas/zipkin.py
import csv

from locust import HttpUser, events, task

REPETICOES = 30
URL = "/api/v2/traces"
BASE = {"serviceName": "tcc-teste", "lookback": 3600000, "limit": 200}

arquivo = open("/mnt/locust/tempos_zipkin.csv", "w", newline="")
saida = csv.writer(arquivo)
saida.writerow(["inicio", "consulta", "ms", "status", "erro"])


@events.request.add_listener
def registrar(name, response_time, response, exception=None, start_time=None, **kwargs):
    saida.writerow([start_time, name, round(response_time, 3),
                    getattr(response, "status_code", None), exception or ""])


class ConsultaZipkin(HttpUser):
    host = "http://zipkin:9411"
    repeticoes = 0

    def consultar(self, nome, params, traces, spans):
        with self.client.get(URL, params=params, name=nome, catch_response=True) as resposta:
            if resposta.status_code != 200:
                resposta.failure(f"status {resposta.status_code}")
                return
            lista = resposta.json()
            obtidos = (len(lista), sum(len(t) for t in lista))
            if obtidos != (traces, spans):
                resposta.failure(f"{obtidos[0]} traces e {obtidos[1]} spans, "
                                 f"esperado {traces} e {spans}")

    @task
    def consultas(self):
        self.consultar("busca_servico", BASE, 100, 600)
        self.consultar("busca_atributo", {**BASE, "annotationQuery": "tcc.busca=alvo"}, 100, 600)
        self.repeticoes += 1
        if self.repeticoes == REPETICOES:
            arquivo.close()
            self.environment.runner.quit()