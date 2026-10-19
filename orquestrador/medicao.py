"""CPU e memória dos contêineres, lidos do cAdvisor a cada segundo.

Uso, a partir da pasta orquestrador, para testar a coleta com o Loki:
    python medicao.py teste
"""
import csv
import re
import subprocess
import threading
import time

import requests

from config import ARQUIVO_VERSOES, HOST_BACKEND, RAIZ, arquivos_compose, docker_em

PASTA = RAIZ / "monitoramento" / "cadvisor"
RESULTADOS = RAIZ / "resultados"
METRICAS = {
    "container_cpu_usage_seconds_total": "cpu_s",         # tempo de CPU acumulado
    "container_memory_working_set_bytes": "memoria_bytes",  # memória em uso, sem o cache liberável
}
NOME = re.compile(r'name="([^"]+)"')
IGNORADOS = {"cadvisor"}


def _compose(host, *args):
    subprocess.run(
        ["docker", "compose", "--env-file", str(ARQUIVO_VERSOES), *arquivos_compose(PASTA), *args],
        cwd=PASTA, env=docker_em(host), check=True, capture_output=True,
    )


def subir_cadvisor(host=HOST_BACKEND, espera=60):
    """Sobe o cAdvisor, se ainda não estiver no ar, e espera ele responder."""
    _compose(host, "up", "-d")
    limite = time.time() + espera
    while time.time() < limite:
        try:
            if requests.get(f"http://{host}:8080/healthz", timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise TimeoutError(f"cAdvisor em {host} não respondeu em {espera} s")


def ler(host=HOST_BACKEND):
    """Uma leitura de todos os contêineres: {nome: {"cpu_s": ..., "memoria_bytes": ...}}."""
    texto = requests.get(f"http://{host}:8080/metrics", timeout=5).text
    valores = {}
    for linha in texto.splitlines():
        if linha.startswith("#"):
            continue
        metrica = linha.split("{", 1)[0]
        if metrica not in METRICAS:
            continue
        if 'cpu="' in linha and 'cpu="total"' not in linha:
            continue  # ignora linhas por núcleo, se existirem
        nome = NOME.search(linha)
        if not nome or nome.group(1) in IGNORADOS:
            continue
        valor = float(linha.rsplit("}", 1)[1].split()[0])
        valores.setdefault(nome.group(1), {})[METRICAS[metrica]] = valor
    return valores


class Medicao:
    """Grava uma linha por contêiner a cada intervalo, do início ao fim do bloco with.

        with Medicao(arquivo):
            aplicar_carga()
    """

    def __init__(self, arquivo, host=HOST_BACKEND, intervalo=1.0):
        self.arquivo = arquivo
        self.host = host
        self.intervalo = intervalo
        self.falhas = 0
        self._parar = threading.Event()
        self._thread = threading.Thread(target=self._executar, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *erro):
        self._parar.set()
        self._thread.join()

    def _executar(self):
        with open(self.arquivo, "w", newline="") as arquivo:
            saida = csv.writer(arquivo)
            saida.writerow(["instante", "conteiner", "cpu_s", "memoria_bytes"])
            proximo = time.monotonic()
            while not self._parar.is_set():
                instante = round(time.time(), 3)
                try:
                    for conteiner, v in sorted(ler(self.host).items()):
                        if "cpu_s" in v and "memoria_bytes" in v:
                            saida.writerow([instante, conteiner, v["cpu_s"], v["memoria_bytes"]])
                except requests.RequestException:
                    self.falhas += 1  # uma leitura perdida não interrompe a execução
                proximo += self.intervalo
                self._parar.wait(max(0.0, proximo - time.monotonic()))


def resumir(arquivo, descarte=0.0):
    """CPU média e memória por contêiner, descartando os primeiros segundos."""
    with open(arquivo) as f:
        linhas = list(csv.DictReader(f))
    if not linhas:
        return {}
    inicio = float(linhas[0]["instante"]) + descarte
    por_conteiner = {}
    for linha in linhas:
        if float(linha["instante"]) >= inicio:
            por_conteiner.setdefault(linha["conteiner"], []).append(linha)
    resumo = {}
    for conteiner, ls in por_conteiner.items():
        t0, t1 = float(ls[0]["instante"]), float(ls[-1]["instante"])
        cpu = None
        if t1 > t0:
            # o contador acumulado dividido pelo tempo dá a média exata, mesmo com leituras repetidas
            cpu = (float(ls[-1]["cpu_s"]) - float(ls[0]["cpu_s"])) / (t1 - t0) * 100
        memoria = sorted(float(l["memoria_bytes"]) for l in ls)
        resumo[conteiner] = {
            "amostras": len(ls),
            "duracao_s": round(t1 - t0, 1),
            "cpu_pct": round(cpu, 1) if cpu is not None else None,
            "memoria_mediana_mib": round(memoria[len(memoria) // 2] / 2**20, 1),
            "memoria_max_mib": round(memoria[-1] / 2**20, 1),
        }
    return resumo


def teste():
    """Mede o Loki recebendo 1000 logs por segundo durante 30 segundos."""
    import collector
    import geradores
    from backends.loki import Loki

    RESULTADOS.mkdir(exist_ok=True)
    arquivo = RESULTADOS / "teste_medicao.csv"
    loki = Loki()
    print("subindo cAdvisor, Loki e Collector")
    subir_cadvisor()
    collector.parar()
    loki.limpar()
    loki.subir()
    collector.subir("logs-loki")
    print("medindo durante a carga")
    with Medicao(arquivo) as medicao:
        time.sleep(5)  # alguns segundos em repouso antes da carga
        geradores.telemetrygen("logs", "--logs", "30000", "--rate", "1000",
                               "--body", "teste tcc", "--service", "tcc-teste")
        time.sleep(5)
    collector.parar()
    loki.limpar()
    print(f"leituras perdidas: {medicao.falhas}")
    for conteiner, valores in resumir(arquivo).items():
        print(conteiner, valores)
    print(f"dados brutos em {arquivo}")


if __name__ == "__main__":
    import sys
    if sys.argv[1:] == ["teste"]:
        teste()
    else:
        sys.exit(__doc__)
