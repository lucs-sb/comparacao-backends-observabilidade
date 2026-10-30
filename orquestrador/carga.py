"""Geração de carga e leitura de contadores, comuns à saturação, à ingestão e à cardinalidade.

Nos pares de logs e de traces, um telemetrygen por serviço envia ao Collector, e a taxa
entregue é lida nos contadores do Collector. No par de métricas, vários avalanche enviam
direto ao backend por escrita remota, e a taxa é lida nos contadores do próprio backend.

Os geradores rodam em segundo plano, como contêineres da instância 3, e são removidos pelo
nome ao fim de cada carga. As leituras de CPU da instância 3 mostram se foi o gerador, e não
o backend, que deixou de acompanhar a taxa pedida.
"""
import csv
import subprocess
import sys
import time

import requests

import formato
import geradores
from config import HOST_CARGA, HOST_DEMO, hosts_extras
from execucao import FERRAMENTAS
from medicao import ler

AMOSTRAGEM = 5      # segundos entre leituras dos contadores
TOLERANCIA = 0.95   # abaixo de 95% do esperado, a taxa é considerada não atingida
METRICAS_COLLECTOR = f"http://{HOST_DEMO}:8888/metrics"
CONTEINER = "tcc-gerador"
AVALANCHE = "tcc-avalanche"


# ---------------------------------------------------------------- logs e traces

def ler_collector(sinal):
    """Contadores e fila do Collector, somados entre receptores e exportadores.

    Devolve aceitos e recusados pelos receptores, enviados e falhas dos exportadores, falhas
    por fila cheia e a ocupação e a capacidade da fila. Os contadores são acumulados desde a
    subida do Collector, então a taxa sai da diferença entre duas leituras.
    """
    nomes = {
        "aceitos": f"otelcol_receiver_accepted_{sinal}",
        "recusados": f"otelcol_receiver_refused_{sinal}",
        "enviados": f"otelcol_exporter_sent_{sinal}",
        "falhas_envio": f"otelcol_exporter_send_failed_{sinal}",
        "falhas_fila": f"otelcol_exporter_enqueue_failed_{sinal}",
        "fila": "otelcol_exporter_queue_size",
        "capacidade_fila": "otelcol_exporter_queue_capacity",
    }
    valores = dict.fromkeys(nomes, 0.0)
    for linha in requests.get(METRICAS_COLLECTOR, timeout=5).text.splitlines():
        if linha.startswith("#"):
            continue
        for chave, nome in nomes.items():
            if linha.startswith(nome):
                valores[chave] += float(linha.rsplit(" ", 1)[1])
    return valores


def _nomes_telemetrygen():
    """Nomes dos contêineres do telemetrygen, um por serviço."""
    return [f"{CONTEINER}-{i}" for i in range(1, len(formato.SERVICOS) + 1)]


def iniciar_telemetrygen(par, taxa, duracao, registro):
    """Um telemetrygen por serviço, em segundo plano, com a taxa dividida igualmente.

    A saída vai para um arquivo. Com um pipe que ninguém lê, o telemetrygen trava ao
    encher o buffer e nunca termina.
    """
    remover_geradores()
    taxa_por_servico = taxa / len(formato.SERVICOS)
    processos = []
    for nome, servico in zip(_nomes_telemetrygen(), formato.SERVICOS):
        processos.append(subprocess.Popen(
            ["docker", "run", "--rm", "--name", nome, "--network", "tcc", *hosts_extras(),
             geradores.TELEMETRYGEN,
             *formato.argumentos(par, servico, taxa_por_servico, duracao),
             "--otlp-endpoint", "otelcol:4317", "--otlp-insecure"],
            stdout=registro, stderr=subprocess.STDOUT,
        ))
    return processos


def remover_geradores():
    """Remove todos os contêineres de gerador desta máquina, rodando ou não.

    Os nomes do avalanche vão até 20, para pegar sobras de uma execução com mais geradores.
    """
    avalanche = [f"{AVALANCHE}-{i}" for i in range(1, 21)]
    subprocess.run(["docker", "rm", "-f", *_nomes_telemetrygen(), *avalanche],
                   capture_output=True)


# ---------------------------------------------------------------- métricas
# O número de séries fica fixo, para não misturar saturação com cardinalidade, e a taxa sobe
# encurtando o intervalo de envio. Cada contêiner do avalanche leva um rótulo gerador próprio,
# então as séries dos geradores não se sobrepõem.
METRICAS = 1000
SERIES_POR_METRICA = 100
SERIES = METRICAS * SERIES_POR_METRICA  # 100 mil séries, divididas entre os geradores
GERADORES_METRICAS = 8                  # muda com --geradores, sempre um divisor de 1000
METRICAS_POR_GERADOR = METRICAS // GERADORES_METRICAS
PORTAS = {"prometheus": 9090, "victoriametrics": 8428}

# Contadores do backend: aceitas são as amostras recebidas pela escrita remota e gravadas, as
# que chegaram ao armazenamento. No Prometheus as duas coincidem, porque a amostra é gravada
# na própria requisição. No VictoriaMetrics, a gravação passa por um buffer em memória.
CONTADORES = {
    "prometheus": {"aceitas": ("prometheus_tsdb_head_samples_appended_total",),
                   "gravadas": ("prometheus_tsdb_head_samples_appended_total",)},
    "victoriametrics": {"aceitas": ('vm_rows_inserted_total{type="promremotewrite"',),
                        "gravadas": ("vm_rows_added_to_storage_total",),
                        # requisições que esperaram vaga no limite de inserções simultâneas,
                        # que por padrão é o dobro do número de núcleos
                        "esperas": ("vm_concurrent_insert_limit_reached_total",)},
}


def configurar_geradores(n):
    """Muda o número de contêineres do avalanche, que precisa dividir as 1000 métricas."""
    global GERADORES_METRICAS, METRICAS_POR_GERADOR
    if METRICAS % n:
        sys.exit(f"--geradores precisa dividir {METRICAS}")
    GERADORES_METRICAS = n
    METRICAS_POR_GERADOR = METRICAS // n


def _nomes_avalanche():
    """Nomes dos contêineres do avalanche, um por gerador."""
    return [f"{AVALANCHE}-{i}" for i in range(1, GERADORES_METRICAS + 1)]


def intervalo_ms(taxa):
    """Intervalo de envio, em milissegundos, que dá a taxa pedida com o número de séries fixo.

    Cada envio leva uma amostra de cada série, então a taxa em amostras por segundo é o
    número de séries dividido pelo intervalo. O arredondamento faz a taxa real diferir um
    pouco da pedida, e por isso os experimentos recalculam o alvo a partir do intervalo.
    """
    return max(1, round(SERIES * 1000 / taxa))


def _erro_de_escrita(ferramenta, linha):
    """Se a linha do /metrics conta requisições de escrita remota que terminaram em erro."""
    if ferramenta == "prometheus":
        return (linha.startswith("prometheus_http_requests_total{")
                and 'handler="/api/v1/write"' in linha and 'code="2' not in linha)
    return linha.startswith("vm_http_request_errors_total{") and "/api/v1/write" in linha


def ler_backend(ferramenta):
    """Amostras aceitas e gravadas e erros de escrita, lidos no /metrics do backend.

    Os prefixos de cada contador estão em CONTADORES. No VictoriaMetrics, esperas conta as
    requisições que aguardaram vaga para inserir, um sinal de que ele está perto do limite.
    """
    url = FERRAMENTAS[ferramenta][0]().url + "/metrics"
    valores = {"aceitas": 0.0, "gravadas": 0.0, "erros": 0.0, "esperas": 0.0}
    for linha in requests.get(url, timeout=5).text.splitlines():
        if not linha or linha.startswith("#"):
            continue
        try:
            valor = float(linha.rsplit(" ", 1)[1])   # o valor vem depois do último espaço
        except (IndexError, ValueError):
            continue
        for chave, prefixos in CONTADORES[ferramenta].items():
            if linha.startswith(prefixos):
                valores[chave] += valor
        if _erro_de_escrita(ferramenta, linha):
            valores["erros"] += valor
    return valores


def cpu_geradores():
    """Tempo de CPU acumulado dos contêineres do avalanche, lido no cAdvisor da instância 3."""
    try:
        leitura = ler(HOST_CARGA)
    except requests.RequestException:
        return None
    return sum(v.get("cpu_s", 0.0) for nome, v in leitura.items() if nome.startswith(AVALANCHE))


def baixar_avalanche():
    """Baixa a imagem antes da carga, para o download não cair dentro da medição."""
    subprocess.run(["docker", "pull", geradores.AVALANCHE], check=True, capture_output=True)


def iniciar_avalanche(ferramenta, intervalo, registro, series_por_metrica=None, valores_s=1):
    """Os avalanche em segundo plano, enviando sem fim até serem removidos.

    Cada gerador cria METRICAS_POR_GERADOR métricas com series_por_metrica séries cada e envia
    todas a cada intervalo, em milissegundos. A cardinalidade muda series_por_metrica para
    variar o total de séries. Os valores mudam a cada valores_s segundos, como numa coleta
    real. A ordem fora do tempo vem ligada por padrão e mandaria amostras com até 5 minutos
    de atraso, que o Prometheus recusa, então ela é desligada. Devolve os processos.
    """
    series_por_metrica = series_por_metrica or SERIES_POR_METRICA
    remover_geradores()
    processos = []
    for i, nome in enumerate(_nomes_avalanche(), start=1):
        processos.append(subprocess.Popen(
            ["docker", "run", "--rm", "--name", nome, "--network", "tcc", *hosts_extras(),
             geradores.AVALANCHE,
             f"--gauge-metric-count={METRICAS_POR_GERADOR}",
             f"--series-count={series_por_metrica}",
             "--series-interval=0", "--metric-interval=0", f"--value-interval={valores_s}",
             f"--const-label=gerador={i}",           # separa as séries de cada gerador
             f"--remote-url=http://{ferramenta}:{PORTAS[ferramenta]}/api/v1/write",
             f"--remote-write-interval={intervalo}ms",
             "--remote-requests-count=-1",           # envia até ser removido
             "--no-remote-out-of-order"],
            stdout=registro, stderr=subprocess.STDOUT,
        ))
    return processos


def encerrar(processos, espera=30):
    """Espera os processos dos geradores removidos terminarem, e força se demorarem."""
    for processo in processos:
        try:
            processo.wait(timeout=espera)
        except subprocess.TimeoutExpired:
            processo.kill()


# ---------------------------------------------------------------- instância 3 e amostras

def cpu_instancia():
    """Tempo ocupado e total de todos os núcleos desta máquina, lidos em /proc/stat.

    A primeira linha do /proc/stat soma todos os núcleos, em unidades de tempo, separados por
    estado. Ocioso é a soma dos estados idle e iowait, e o resto conta como ocupado.
    """
    try:
        with open("/proc/stat") as f:
            campos = [int(x) for x in f.readline().split()[1:]]
    except OSError:
        return None  # fora do Linux, como no MacBook
    ocioso = campos[3] + campos[4]  # idle e iowait
    return sum(campos) - ocioso, sum(campos)


def pct_instancia(inicio, fim):
    """CPU ocupada da instância 3 entre duas leituras de cpu_instancia, em porcentagem."""
    if not inicio or not fim or fim[1] <= inicio[1]:
        return None
    return (fim[0] - inicio[0]) / (fim[1] - inicio[1]) * 100


def registrar_amostra(pasta, degrau, instante, valores):
    """Guarda cada leitura em amostras.csv, para ver a evolução da carga ao longo do tempo."""
    arquivo = pasta / "amostras.csv"
    novo = not arquivo.exists()
    with open(arquivo, "a", newline="") as f:
        saida = csv.writer(f)
        if novo:
            saida.writerow(["degrau", "instante", *valores])
        saida.writerow([degrau, round(instante, 3), *valores.values()])


def amostrar(leitor, pasta, duracao, inicio):
    """Lê os contadores a cada AMOSTRAGEM segundos até o fim da carga.

    leitor é uma função sem argumentos que devolve um dicionário de contadores, como
    ler_collector ou ler_backend. Devolve a lista de pares (instante, contadores) e grava
    cada leitura em amostras.csv.
    """
    amostras = []
    while time.time() < inicio + duracao:
        try:
            leitura = (time.time(), leitor())
        except requests.RequestException:
            time.sleep(AMOSTRAGEM)  # uma leitura perdida não interrompe a execução
            continue
        amostras.append(leitura)
        registrar_amostra(pasta, 1, *leitura)
        time.sleep(AMOSTRAGEM)
    return amostras
