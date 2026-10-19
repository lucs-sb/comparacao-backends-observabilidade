"""Saturação nos pares de logs e traces: carga em degraus até o backend não acompanhar.

Uso, a partir da pasta orquestrador:
    python saturacao.py rampa <ferramenta> [--inicio 1000] [--degrau 60]
        Piloto (P1): dobra a taxa a cada degrau, a partir de --inicio itens por segundo.
    python saturacao.py degraus <ferramenta> --estimativa N [--repeticao 1] [--degrau 180]
        Experimento: de 50% a 150% da estimativa, com incremento de 10% (seção 4.7).

Ferramentas: loki, elasticsearch, jaeger, zipkin. O par de métricas fica para uma peça própria (A13).
"""
import argparse
import csv
import json
import subprocess
import sys
import time

import requests

import geradores
from config import HOST_DEMO, docker_em, hosts_extras
from execucao import SINAIS, Execucao, executar

PARES = {"loki": "logs", "elasticsearch": "logs", "jaeger": "traces", "zipkin": "traces"}
WORKERS = 4         # processos do telemetrygen, a taxa é dividida entre eles
ACOMODACAO = 10     # segundos ignorados no início e no fim de cada degrau
AMOSTRAGEM = 5      # segundos entre leituras do Collector dentro do degrau
TOLERANCIA = 0.95   # abaixo de 95% do esperado, a taxa é considerada não atingida
CORPO_LOG = "teste tcc"  # formato provisório, o definitivo sai de P6
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


def _iniciar_telemetrygen(par, taxa, duracao):
    """Roda o telemetrygen em segundo plano, com a taxa total dividida entre os workers."""
    por_worker = max(1, round(taxa / WORKERS))
    comum = ["--rate", str(por_worker), "--workers", str(WORKERS),
             "--duration", f"{duracao}s", "--service", "tcc-teste"]
    if par == "logs":
        args = ["logs", *comum, "--body", CORPO_LOG]
    else:
        # 1 span pai e 5 filhos por trace; a taxa do telemetrygen conta spans (A1)
        args = ["traces", *comum, "--child-spans", "5"]
    subprocess.run(["docker", "rm", "-f", CONTEINER], capture_output=True)
    return subprocess.Popen(
        ["docker", "run", "--rm", "--name", CONTEINER, "--network", "tcc", *hosts_extras(),
         geradores.TELEMETRYGEN, *args, "--otlp-endpoint", "otelcol:4317", "--otlp-insecure"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )


def avaliar(amostras, alvo):
    """Classifica um degrau pelas leituras do Collector feitas durante ele."""
    (t0, a), (t1, b) = amostras[0], amostras[-1]
    duracao = t1 - t0
    aceita = (b["aceitos"] - a["aceitos"]) / duracao
    enviada = (b["enviados"] - a["enviados"]) / duracao
    falhas = (b["falhas_envio"] - a["falhas_envio"]) + (b["falhas_fila"] - a["falhas_fila"])
    recusados = b["recusados"] - a["recusados"]

    # fila crescente: o último terço do degrau tem fila média maior que o primeiro terço,
    # com diferença acima de 5% da capacidade
    filas = [v["fila"] for _, v in amostras]
    terco = max(1, len(filas) // 3)
    capacidade = b["capacidade_fila"]
    crescimento = sum(filas[-terco:]) / terco - sum(filas[:terco]) / terco
    fila_crescente = capacidade > 0 and crescimento > 0.05 * capacidade

    if recusados > 0:
        estado = "recusado"            # o próprio Collector recusou dados, limite de memória
    elif falhas > 0 or fila_crescente or enviada < TOLERANCIA * aceita:
        estado = "saturado"            # o backend não acompanhou o que o Collector recebeu
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
            processo = _iniciar_telemetrygen(par, taxa, duracao)
            time.sleep(ACOMODACAO)
            amostras = []
            fim = inicio + duracao - ACOMODACAO
            while time.time() < fim:
                amostras.append((time.time(), ler_collector(sinal)))
                time.sleep(AMOSTRAGEM)
            processo.wait()
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
    validos = [d for d in degraus if d["estado"] == "ok"]
    conclusao = {
        "vazao_maxima": validos[-1]["aceita"] if validos else None,
        "parada": degraus[-1]["estado"] if degraus else None,
        "degraus": len(degraus),
    }
    with open(pasta / "saturacao.json", "w") as arquivo:
        json.dump(conclusao, arquivo, indent=2, ensure_ascii=False)
    return conclusao


def executar_saturacao(ferramenta, experimento, condicao, repeticao, taxas, duracao):
    par = PARES[ferramenta]
    execucao = Execucao(experimento, par, ferramenta, condicao, repeticao, aquecimento_s=0)
    conclusao = {}

    def carga():
        conclusao.update(rodar_degraus(par, taxas, duracao, execucao.pasta))

    try:
        executar(execucao, carga)
    finally:
        subprocess.run(["docker", "rm", "-f", CONTEINER], capture_output=True)
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
    args = parser.parse_args()

    if args.modo == "rampa":
        taxas = [args.inicio * 2 ** k for k in range(12)]  # até 2048 vezes o início
        executar_saturacao(args.ferramenta, "rampa", f"inicio{round(args.inicio)}",
                           args.repeticao, taxas, args.degrau or 60)
    else:
        if not args.estimativa:
            sys.exit("informe --estimativa, a vazão estimada na rampa")
        taxas = [args.estimativa * f / 10 for f in range(5, 16)]  # 50% a 150%
        executar_saturacao(args.ferramenta, "saturacao", "degraus", args.repeticao,
                           taxas, args.degrau or 180)


if __name__ == "__main__":
    main()
