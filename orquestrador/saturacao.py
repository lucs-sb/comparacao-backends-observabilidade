"""Saturação dos três pares: carga em degraus até o backend não acompanhar.

Uso, a partir da pasta orquestrador:
    python saturacao.py rampa <ferramenta> [--inicio 1000] [--degrau 60]
        Piloto (P1): dobra a taxa a cada degrau, a partir de --inicio itens por segundo.
    python saturacao.py degraus <ferramenta> --estimativa N [--repeticao 1] [--degrau 180]
        Experimento: de 50% a 150% da estimativa, com incremento de 10% (seção 4.7).

Ferramentas: loki, elasticsearch, jaeger, zipkin, prometheus, victoriametrics.
Em logs e traces, a carga passa pelo Collector e a saturação é lida nos contadores dele.
Em métricas, o avalanche envia direto ao backend e a saturação é lida nos contadores do
próprio backend (A13). Nas métricas, os itens são amostras.
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import time

import requests

import formato
import geradores
from config import HOST_CARGA, HOST_DEMO, docker_em, hosts_extras
from execucao import FERRAMENTAS, SINAIS, Execucao, executar
from medicao import Medicao, ler, subir_cadvisor

PARES = {"loki": "logs", "elasticsearch": "logs", "jaeger": "traces", "zipkin": "traces",
         "prometheus": "metricas", "victoriametrics": "metricas"}
ACOMODACAO = 10     # segundos ignorados no início e no fim de cada degrau
AMOSTRAGEM = 5      # segundos entre leituras do Collector dentro do degrau
TOLERANCIA = 0.95   # abaixo de 95% do esperado, a taxa é considerada não atingida
CONTEINER = "tcc-gerador"
METRICAS_COLLECTOR = f"http://{HOST_DEMO}:8888/metrics"


def ler_collector(sinal):
    """Contadores e fila do Collector, somados entre receptores e exportadores."""
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


def _nomes():
    return [f"{CONTEINER}-{i}" for i in range(1, len(formato.SERVICOS) + 1)]


def _remover_geradores():
    # os nomes do avalanche vão até 20, para pegar sobras de uma execução com mais geradores
    avalanche = [f"{AVALANCHE}-{i}" for i in range(1, 21)]
    subprocess.run(["docker", "rm", "-f", *_nomes(), *avalanche], capture_output=True)


def _iniciar_geradores(par, taxa, duracao, registro):
    """Um telemetrygen por serviço, em segundo plano, com a taxa dividida igualmente.

    A saída vai para um arquivo. Com um pipe que ninguém lê, o telemetrygen trava ao
    encher o buffer e nunca termina.
    """
    _remover_geradores()
    taxa_por_servico = taxa / len(formato.SERVICOS)
    processos = []
    for nome, servico in zip(_nomes(), formato.SERVICOS):
        processos.append(subprocess.Popen(
            ["docker", "run", "--rm", "--name", nome, "--network", "tcc", *hosts_extras(),
             geradores.TELEMETRYGEN,
             *formato.argumentos(par, servico, taxa_por_servico, duracao),
             "--otlp-endpoint", "otelcol:4317", "--otlp-insecure"],
            stdout=registro, stderr=subprocess.STDOUT,
        ))
    return processos


def avaliar(amostras, alvo):
    """Classifica um degrau pelas leituras do Collector feitas durante ele."""
    (t0, a), (t1, b) = amostras[0], amostras[-1]
    duracao = t1 - t0
    aceita = (b["aceitos"] - a["aceitos"]) / duracao
    enviada = (b["enviados"] - a["enviados"]) / duracao
    falhas = (b["falhas_envio"] - a["falhas_envio"]) + (b["falhas_fila"] - a["falhas_fila"])
    recusados = b["recusados"] - a["recusados"]

    # Fila cheia: no último terço do degrau, a fila média ocupa 80% ou mais da capacidade.
    # O acúmulo em si já aparece em "enviada abaixo de aceita". Uma regra baseada só em
    # crescimento disparava com oscilações de um ou dois lotes em filas pequenas, como a de
    # 10 lotes do exportador do Elasticsearch.
    filas = [v["fila"] for _, v in amostras]
    terco = max(1, len(filas) // 3)
    capacidade = b["capacidade_fila"]
    fila_cheia = capacidade > 0 and sum(filas[-terco:]) / terco >= 0.8 * capacidade

    # A saturação do backend vem primeiro: quando ele não acompanha, o Collector acumula
    # dados até o limite de memória e passa a recusar, mas a causa continua sendo o backend.
    if falhas > 0 or fila_cheia or enviada < TOLERANCIA * aceita:
        estado = "saturado"            # o backend não acompanhou o que o Collector recebeu
    elif recusados > 0:
        estado = "recusado"            # o Collector recusou dados com o backend em dia
    elif aceita < TOLERANCIA * alvo:
        estado = "gerador_limitado"    # a taxa pedida não foi atingida (P5)
    else:
        estado = "ok"
    return {
        "alvo": round(alvo), "aceita": round(aceita), "enviada": round(enviada),
        "falhas": int(falhas), "recusados": int(recusados),
        "fila_inicio": round(filas[0]), "fila_fim": round(filas[-1]),
        "capacidade_fila": round(capacidade), "estado": estado,
    }


def rodar_degraus(par, taxas, duracao, pasta):
    """Aplica as taxas em sequência e para no primeiro degrau que não for ok."""
    sinal = SINAIS[par]
    degraus = []
    with open(pasta / "degraus.csv", "w", newline="") as arquivo:
        saida = None
        for numero, taxa in enumerate(taxas, start=1):
            print(f"  degrau {numero}: {round(taxa)} itens/s por {duracao} s", flush=True)
            inicio = time.time()
            registro = open(pasta / f"gerador_degrau{numero}.log", "w")
            processos = _iniciar_geradores(par, taxa, duracao, registro)
            time.sleep(ACOMODACAO)
            amostras = []
            fim = inicio + duracao - ACOMODACAO
            while time.time() < fim:
                amostras.append((time.time(), ler_collector(sinal)))
                time.sleep(AMOSTRAGEM)
            limite = inicio + duracao + 60
            try:
                for processo in processos:
                    processo.wait(timeout=max(1, limite - time.time()))
            except subprocess.TimeoutExpired:
                _remover_geradores()
                raise RuntimeError(f"os geradores não terminaram no degrau {numero}, "
                                   f"ver gerador_degrau{numero}.log")
            finally:
                registro.close()
            resultado = {"degrau": numero, "inicio": round(inicio, 3), "fim": round(time.time(), 3),
                         **avaliar(amostras, taxa)}
            degraus.append(resultado)
            if saida is None:
                saida = csv.DictWriter(arquivo, fieldnames=list(resultado))
                saida.writeheader()
            saida.writerow(resultado)
            arquivo.flush()
            print(f"    aceita {resultado['aceita']}, enviada {resultado['enviada']}, "
                  f"fila {resultado['fila_inicio']} para {resultado['fila_fim']}: "
                  f"{resultado['estado']}", flush=True)
            if resultado["estado"] != "ok":
                break
    return _concluir(degraus, pasta)


def _concluir(degraus, pasta):
    validos = [d for d in degraus if d["estado"] == "ok"]
    conclusao = {
        "vazao_maxima": validos[-1]["aceita"] if validos else None,
        "parada": degraus[-1]["estado"] if degraus else None,
        "degraus": len(degraus),
    }
    with open(pasta / "saturacao.json", "w") as arquivo:
        json.dump(conclusao, arquivo, indent=2, ensure_ascii=False)
    return conclusao


# ---------------------------------------------------------------- métricas (A13)
# O número de séries fica fixo, para não misturar saturação com cardinalidade, e a taxa sobe
# encurtando o intervalo de envio. Cada contêiner do avalanche leva um rótulo gerador próprio,
# então as séries dos quatro não se sobrepõem.
AVALANCHE = "tcc-avalanche"
METRICAS = 1000
SERIES_POR_METRICA = 100
SERIES = METRICAS * SERIES_POR_METRICA  # 100 mil séries, divididas entre os geradores
GERADORES_METRICAS = 4                  # muda com --geradores, sempre um divisor de 1000
METRICAS_POR_GERADOR = METRICAS // GERADORES_METRICAS
PORTAS = {"prometheus": 9090, "victoriametrics": 8428}
LIMITE_GERADOR = 0.9  # fração da CPU da instância 3 a partir da qual o gerador é o limite (P5)

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
    global GERADORES_METRICAS, METRICAS_POR_GERADOR
    if METRICAS % n:
        sys.exit(f"--geradores precisa dividir {METRICAS}")
    GERADORES_METRICAS = n
    METRICAS_POR_GERADOR = METRICAS // n


def _nomes_avalanche():
    return [f"{AVALANCHE}-{i}" for i in range(1, GERADORES_METRICAS + 1)]


def intervalo_ms(taxa):
    """Intervalo de envio que dá a taxa pedida com o número de séries fixo."""
    return max(1, round(SERIES * 1000 / taxa))


def _erro_de_escrita(ferramenta, linha):
    if ferramenta == "prometheus":
        return (linha.startswith("prometheus_http_requests_total{")
                and 'handler="/api/v1/write"' in linha and 'code="2' not in linha)
    return linha.startswith("vm_http_request_errors_total{") and "/api/v1/write" in linha


def ler_backend(ferramenta):
    """Amostras aceitas e gravadas e erros de escrita, lidos no /metrics do backend."""
    url = FERRAMENTAS[ferramenta][0]().url + "/metrics"
    valores = {"aceitas": 0.0, "gravadas": 0.0, "erros": 0.0, "esperas": 0.0}
    for linha in requests.get(url, timeout=5).text.splitlines():
        if not linha or linha.startswith("#"):
            continue
        try:
            valor = float(linha.rsplit(" ", 1)[1])
        except (IndexError, ValueError):
            continue
        for chave, prefixos in CONTADORES[ferramenta].items():
            if linha.startswith(prefixos):
                valores[chave] += valor
        if _erro_de_escrita(ferramenta, linha):
            valores["erros"] += valor
    return valores


def _cpu_geradores():
    """Tempo de CPU acumulado dos contêineres do avalanche, lido no cAdvisor da instância 3."""
    try:
        leitura = ler(HOST_CARGA)
    except requests.RequestException:
        return None
    return sum(v.get("cpu_s", 0.0) for nome, v in leitura.items() if nome.startswith(AVALANCHE))


def _cpu_instancia():
    """Tempo ocupado e total de todos os núcleos desta máquina, lidos em /proc/stat."""
    try:
        with open("/proc/stat") as f:
            campos = [int(x) for x in f.readline().split()[1:]]
    except OSError:
        return None  # fora do Linux, como no MacBook
    ocioso = campos[3] + campos[4]  # idle e iowait
    return sum(campos) - ocioso, sum(campos)


def _iniciar_avalanche(ferramenta, intervalo, registro):
    """Os avalanche em segundo plano, enviando sem fim até serem removidos.

    A ordem fora do tempo vem ligada por padrão e mandaria amostras com até 5 minutos de
    atraso, que o Prometheus recusa. Os valores mudam a cada segundo, como numa coleta real.
    """
    _remover_geradores()
    processos = []
    for i, nome in enumerate(_nomes_avalanche(), start=1):
        processos.append(subprocess.Popen(
            ["docker", "run", "--rm", "--name", nome, "--network", "tcc", *hosts_extras(),
             geradores.AVALANCHE,
             f"--gauge-metric-count={METRICAS_POR_GERADOR}",
             f"--series-count={SERIES_POR_METRICA}",
             "--series-interval=0", "--metric-interval=0", "--value-interval=1",
             f"--const-label=gerador={i}",
             f"--remote-url=http://{ferramenta}:{PORTAS[ferramenta]}/api/v1/write",
             f"--remote-write-interval={intervalo}ms",
             "--remote-requests-count=-1",
             "--no-remote-out-of-order"],
            stdout=registro, stderr=subprocess.STDOUT,
        ))
    return processos


def avaliar_metricas(amostras, alvo, cpu_gerador, cpu_instancia, encerrados):
    """Classifica um degrau pelas leituras do backend feitas durante ele.

    O avalanche espera cada envio terminar antes do próximo, então um backend lento reduz a
    taxa em vez de acumular fila. Quando a taxa fica abaixo do alvo, a CPU do gerador separa
    os dois casos: perto do limite da instância 3, quem não acompanhou foi o gerador. A
    medida vale tanto pelos contêineres do avalanche quanto pela instância inteira, que inclui
    o tráfego de rede tratado pelo sistema.
    """
    (t0, a), (t1, b) = amostras[0], amostras[-1]
    duracao = t1 - t0
    aceita = (b["aceitas"] - a["aceitas"]) / duracao
    gravada = (b["gravadas"] - a["gravadas"]) / duracao
    erros = int(b["erros"] - a["erros"]) + encerrados
    esperas = int(b["esperas"] - a["esperas"])
    limite = LIMITE_GERADOR * 100 * os.cpu_count()
    gerador_no_limite = ((cpu_gerador is not None and cpu_gerador >= limite)
                         or (cpu_instancia is not None and cpu_instancia >= 100 * LIMITE_GERADOR))

    if erros > 0 or gravada < TOLERANCIA * aceita:
        estado = "saturado"            # o backend recusou envios ou não gravou o que recebeu
    elif aceita < TOLERANCIA * alvo:
        if gerador_no_limite:
            estado = "gerador_limitado"    # a instância 3 não produziu a taxa pedida (P5)
        else:
            estado = "saturado"            # o backend respondeu devagar e segurou o gerador
    else:
        estado = "ok"
    return {
        "alvo": round(alvo), "aceita": round(aceita), "gravada": round(gravada),
        "erros": erros,
        "esperas_insercao": esperas,
        "cpu_gerador_pct": round(cpu_gerador) if cpu_gerador is not None else None,
        "cpu_instancia3_pct": round(cpu_instancia) if cpu_instancia is not None else None,
        "estado": estado,
    }


def rodar_degraus_metricas(ferramenta, taxas, duracao, pasta):
    """Aplica as taxas em sequência e para no primeiro degrau que não for ok."""
    # a imagem baixada antes, para o download não cair dentro do primeiro degrau
    subprocess.run(["docker", "pull", geradores.AVALANCHE], check=True, capture_output=True)
    degraus = []
    with open(pasta / "degraus.csv", "w", newline="") as arquivo:
        saida = None
        for numero, taxa in enumerate(taxas, start=1):
            intervalo = intervalo_ms(taxa)
            alvo = SERIES * 1000 / intervalo
            print(f"  degrau {numero}: {round(alvo)} amostras/s por {duracao} s "
                  f"({SERIES} séries a cada {intervalo} ms)", flush=True)
            inicio = time.time()
            registro = open(pasta / f"gerador_degrau{numero}.log", "w")
            processos = []
            try:
                processos = _iniciar_avalanche(ferramenta, intervalo, registro)
                time.sleep(ACOMODACAO)
                cpu_inicio = (time.time(), _cpu_geradores())
                instancia_inicio = _cpu_instancia()
                amostras = []
                fim = inicio + duracao - ACOMODACAO
                while time.time() < fim:
                    amostras.append((time.time(), ler_backend(ferramenta)))
                    time.sleep(AMOSTRAGEM)
                cpu_fim = (time.time(), _cpu_geradores())
                instancia_fim = _cpu_instancia()
                # um avalanche que terminou sozinho desistiu depois de mais de 20 erros de envio
                encerrados = sum(1 for p in processos if p.poll() is not None)
            finally:
                _remover_geradores()
                for processo in processos:
                    try:
                        processo.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        processo.kill()
                registro.close()
            cpu = None
            if cpu_inicio[1] is not None and cpu_fim[1] is not None:
                cpu = (cpu_fim[1] - cpu_inicio[1]) / (cpu_fim[0] - cpu_inicio[0]) * 100
            cpu_instancia = None
            if instancia_inicio and instancia_fim and instancia_fim[1] > instancia_inicio[1]:
                cpu_instancia = ((instancia_fim[0] - instancia_inicio[0])
                                 / (instancia_fim[1] - instancia_inicio[1]) * 100)
            resultado = {"degrau": numero, "inicio": round(inicio, 3), "fim": round(time.time(), 3),
                         "intervalo_ms": intervalo,
                         **avaliar_metricas(amostras, alvo, cpu, cpu_instancia, encerrados)}
            degraus.append(resultado)
            if saida is None:
                saida = csv.DictWriter(arquivo, fieldnames=list(resultado))
                saida.writeheader()
            saida.writerow(resultado)
            arquivo.flush()
            print(f"    aceita {resultado['aceita']}, gravada {resultado['gravada']}, "
                  f"erros {resultado['erros']}, esperas {resultado['esperas_insercao']}, "
                  f"CPU do gerador {resultado['cpu_gerador_pct']}%, "
                  f"instância 3 {resultado['cpu_instancia3_pct']}%: "
                  f"{resultado['estado']}", flush=True)
            if resultado["estado"] != "ok":
                break
    return _concluir(degraus, pasta)


def executar_saturacao(ferramenta, experimento, condicao, repeticao, taxas, duracao):
    par = PARES[ferramenta]
    execucao = Execucao(experimento, par, ferramenta, condicao, repeticao, aquecimento_s=0)
    conclusao = {}

    def carga():
        if par == "metricas":
            # o cAdvisor da instância 3 mede o gerador, que é quem separa saturação de limite
            subir_cadvisor(HOST_CARGA)
            with Medicao(execucao.pasta / "recursos_gerador.csv", HOST_CARGA):
                conclusao.update(rodar_degraus_metricas(ferramenta, taxas, duracao,
                                                        execucao.pasta))
        else:
            conclusao.update(rodar_degraus(par, taxas, duracao, execucao.pasta))

    try:
        executar(execucao, carga)
    finally:
        _remover_geradores()
    print(f"\nvazão máxima: {conclusao.get('vazao_maxima')} itens/s, "
          f"parada por: {conclusao.get('parada')}, resultados em {execucao.pasta}")
    return conclusao


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("modo", choices=["rampa", "degraus"])
    parser.add_argument("ferramenta", choices=list(PARES))
    parser.add_argument("--inicio", type=float, default=1000)
    parser.add_argument("--estimativa", type=float)
    parser.add_argument("--repeticao", type=int, default=1)
    parser.add_argument("--degrau", type=int)
    parser.add_argument("--geradores", type=int, default=4,
                        help="contêineres do avalanche no par de métricas")
    args = parser.parse_args()
    configurar_geradores(args.geradores)
    sufixo = f"-g{args.geradores}" if args.geradores != 4 else ""

    if args.modo == "rampa":
        taxas = [args.inicio * 2 ** k for k in range(12)]  # até 2048 vezes o início
        executar_saturacao(args.ferramenta, "rampa", f"inicio{round(args.inicio)}{sufixo}",
                           args.repeticao, taxas, args.degrau or 60)
    else:
        if not args.estimativa:
            sys.exit("informe --estimativa, a vazão estimada na rampa")
        taxas = [args.estimativa * f / 10 for f in range(5, 16)]  # 50% a 150%
        executar_saturacao(args.ferramenta, "saturacao", f"degraus{sufixo}", args.repeticao,
                           taxas, args.degrau or 180)


if __name__ == "__main__":
    main()
