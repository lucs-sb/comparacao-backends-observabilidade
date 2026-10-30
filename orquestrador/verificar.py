"""Verificação do ambiente: cada backend recebe uma carga pequena e conhecida.

Antes dos experimentos, confirma que cada backend sobe, recebe os dados pelo caminho usado no
experimento, guarda exatamente o que foi enviado e tem o disco estável depois da descarga.
Os logs e traces passam pelo Collector isolado, e as métricas vão do avalanche direto ao
backend. Cada caso imprime os critérios, se passou, e deixa os dados no backend para
inspeção.

Uso, a partir da pasta orquestrador:
    python verificar.py --lista        lista os casos
    python verificar.py logs-loki      executa um caso
    python verificar.py parar          derruba todos os backends e o Collector
"""
import argparse
import sys
import time

import collector
import geradores
from backends.elasticsearch import Elasticsearch
from backends.jaeger import Jaeger
from backends.loki import Loki
from backends.prometheus import Prometheus
from backends.victoriametrics import VictoriaMetrics
from backends.zipkin import Zipkin
from execucao import ESPERA_LOTE, TODOS, medir_disco_estavel

# ---------------------------------------------------------------- geradores

def gerar_logs():
    """1000 logs, a 100 por segundo."""
    geradores.telemetrygen("logs", "--logs", "1000", "--rate", "100",
                           "--body", "teste tcc", "--service", "tcc-teste")


def gerar_traces():
    """100 traces com 1 span pai e 5 filhos cada, ou seja, 600 spans."""
    geradores.telemetrygen("traces", "--traces", "100", "--child-spans", "5", "--rate", "10",
                           "--service", "tcc-teste",
                           "--telemetry-attributes", 'tcc.busca="alvo"')


# 100 métricas com 100 séries cada, ou seja, 10 mil séries. Os intervalos zerados impedem o
# avalanche de trocar as séries durante a execução, o que mudaria o total esperado.
SERIES = ["--gauge-metric-count=100", "--series-count=100",
          "--series-interval=0", "--metric-interval=0", "--value-interval=15"]


def avalanche_direto(url):
    """Gerador de métricas que envia direto ao backend no endereço dado."""
    def gerar():
        # 20 envios a cada 15 s, ou seja, 5 minutos
        geradores.avalanche(*SERIES, f"--remote-url={url}",
                            "--remote-write-interval=15s", "--remote-requests-count=20")
    return gerar


# ---------------------------------------------------------------- casos
# nome: (backend, arquivo de par do Collector ou None, sinal do Collector, gerador,
#        itens esperados no backend)

CASOS = {
    "logs-loki": (Loki, "logs-loki", "log_records", gerar_logs, 1000),
    "logs-es": (Elasticsearch, "logs-es", "log_records", gerar_logs, 1000),
    "traces-jaeger": (Jaeger, "traces-jaeger", "spans", gerar_traces, 600),
    "traces-zipkin": (Zipkin, "traces-zipkin", "spans", gerar_traces, 600),
    "metricas-prom": (Prometheus, None, "metric_points",
                      avalanche_direto("http://prometheus:9090/api/v1/write"), 10000),
    "metricas-vm": (VictoriaMetrics, None, "metric_points",
                    avalanche_direto("http://victoriametrics:8428/api/v1/write"), 10000),
}


def parar_tudo():
    """Garante estado limpo: nenhum backend de outro caso rodando e todos os volumes apagados."""
    collector.parar()
    for classe in TODOS:
        classe().limpar()


def verificar(nome):
    """Roda um caso e devolve True se todos os critérios passaram."""
    classe, par, sinal, gerar, esperado = CASOS[nome]
    backend = classe()

    print(f"[{nome}] limpando o ambiente")
    parar_tudo()
    print(f"[{nome}] subindo {backend.pasta}")
    backend.subir()
    if par:
        print(f"[{nome}] subindo o Collector com {par}.yaml")
        collector.subir(par)

    print(f"[{nome}] gerando carga")
    inicio = time.time()
    gerar()
    print(f"[{nome}] carga concluída em {time.time() - inicio:.0f} s, aguardando {ESPERA_LOTE} s")
    time.sleep(ESPERA_LOTE)

    # Lê as contagens do Collector e o desliga antes de medir, para que nada mais
    # chegue ao backend durante a descarga e as medidas de disco.
    enviado = collector.enviados(sinal) if par else None
    falhas = (collector.falhas(sinal) or 0) if par else None
    if par:
        collector.parar()

    backend.descarregar()
    armazenado = backend.contar()
    print(f"[{nome}] medindo o disco até estabilizar")
    disco, estavel, espera_disco = medir_disco_estavel(backend)

    # Os casos de métricas não passam pelo Collector, então só logs e traces
    # comparam enviado com esperado.
    criterios = {
        "armazenado igual ao esperado": armazenado == esperado,
        f"disco estável em {espera_disco:.0f} s": estavel,
    }
    if par:
        criterios["sem falhas no Collector"] = falhas == 0
        if sinal != "metric_points":
            criterios["enviado igual ao esperado"] = enviado == esperado

    print(f"\n[{nome}] esperado={esperado} armazenado={armazenado} "
          f"enviado={enviado if par else '-'} falhas={falhas if par else '-'} "
          f"disco_kib={disco}")
    for criterio, ok in criterios.items():
        print(f"  {'ok   ' if ok else 'FALHA'} {criterio}")
    print(f"[{nome}] {'PASSOU' if all(criterios.values()) else 'NÃO PASSOU'}")
    print("Os dados ficam no backend para inspeção. Use 'python verificar.py parar' ao terminar.")
    return all(criterios.values())


def main():
    """Lê os argumentos e roda um caso, lista os casos ou limpa o ambiente."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("caso", nargs="?", help="nome do caso ou 'parar'")
    parser.add_argument("--lista", action="store_true", help="lista os casos disponíveis")
    args = parser.parse_args()

    if args.lista or not args.caso:
        print("\n".join(CASOS))
        return
    if args.caso == "parar":
        parar_tudo()
        print("Backends e Collector derrubados, volumes apagados.")
        return
    if args.caso not in CASOS:
        sys.exit(f"Caso desconhecido: {args.caso}. Use --lista.")
    sys.exit(0 if verificar(args.caso) else 1)


if __name__ == "__main__":
    main()