# consultas/loki.py
import csv

from locust import HttpUser, events, task

REPETICOES = 30
URL = "/loki/api/v1/query_range"

arquivo = open("/mnt/locust/tempos_loki.csv", "w", newline="")
saida = csv.writer(arquivo)
saida.writerow(["inicio", "consulta", "ms", "status", "erro"])


@events.request.add_listener
def registrar(name, response_time, response, exception=None, start_time=None, **kwargs):
    saida.writerow([start_time, name, round(response_time, 3),
                    getattr(response, "status_code", None), exception or ""])


class ConsultaLoki(HttpUser):
    host = "http://loki:3100"
    repeticoes = 0

    def consultar(self, nome, consulta, esperado):
        with self.client.get(URL, params={"query": consulta, "limit": 5000},
                             name=nome, catch_response=True) as resposta:
            if resposta.status_code != 200:
                resposta.failure(f"status {resposta.status_code}")
                return
            linhas = sum(len(s["values"]) for s in resposta.json()["data"]["result"])
            if linhas != esperado:
                resposta.failure(f"{linhas} linhas, esperado {esperado}")

    @task
    def consultas(self):
        self.consultar("filtro_rotulo", '{service_name="tcc-teste"}', 1000)
        self.consultar("busca_textual", '{service_name="tcc-teste"} |= "teste"', 1000)
        self.repeticoes += 1
        if self.repeticoes == REPETICOES:
            arquivo.close()
            self.environment.runner.quit()