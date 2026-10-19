# consultas/metricas.py
# uso: docker run ... -e ALVO=prometheus      ou      -e ALVO=victoriametrics
import csv
import os
import time

from locust import HttpUser, events, task

REPETICOES = 30
URL = "/api/v1/query_range"
JANELA = 900  # segundos, os últimos 15 minutos
PASSO = 15    # segundos, igual ao intervalo de envio do avalanche
ALVOS = {"prometheus": "http://prometheus:9090",
         "victoriametrics": "http://victoriametrics:8428"}
ALVO = os.environ["ALVO"]

arquivo = open(f"/mnt/locust/tempos_{ALVO}.csv", "w", newline="")
saida = csv.writer(arquivo)
saida.writerow(["inicio", "consulta", "ms", "status", "erro"])


@events.request.add_listener
def registrar(name, response_time, response, exception=None, start_time=None, **kwargs):
    saida.writerow([start_time, name, round(response_time, 3),
                    getattr(response, "status_code", None), exception or ""])


def parametros(consulta):
    fim = time.time()
    return {"query": consulta, "start": fim - JANELA, "end": fim, "step": PASSO}


class ConsultaMetricas(HttpUser):
    host = ALVOS[ALVO]
    repeticoes = 0

    def consultar(self, nome, consulta, esperado):
        with self.client.get(URL, params=parametros(consulta), name=nome,
                             catch_response=True) as resposta:
            if resposta.status_code != 200:
                resposta.failure(f"status {resposta.status_code}")
                return
            series = len(resposta.json()["data"]["result"])
            if series != esperado:
                resposta.failure(f"{series} séries, esperado {esperado}")

    @task
    def consultas(self):
        self.consultar("filtro_rotulo", '{__name__=~"avalanche_.*", series_id="7"}', 100)
        self.consultar("agregacao", 'sum by (series_id) ({__name__=~"avalanche_.*"})', 100)
        self.repeticoes += 1
        if self.repeticoes == REPETICOES:
            arquivo.close()
            self.environment.runner.quit()