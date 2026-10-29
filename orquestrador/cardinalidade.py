"""Cardinalidade: 100 mil, 500 mil ou 1 milhão de séries no par de métricas (seção 4.7).

Uso, a partir da pasta orquestrador:
    python cardinalidade.py <prometheus|victoriametrics> --series 100000 [--repeticao 1]
                            [--duracao 720] [--experimento cardinalidade]

A taxa de amostras é a mesma nos três patamares, cerca de 66,7 mil amostras/s, para que só o
número de séries mude entre eles (A28). Com 1 milhão de séries, isso equivale a uma amostra por
série a cada 15 segundos, o intervalo de coleta padrão do Prometheus. Com menos séries, o
intervalo encurta na mesma proporção: 7,5 s com 500 mil e 1,5 s com 100 mil. Os valores mudam
a cada envio, e o número de séries guardadas é conferido no backend ao final.
"""
import argparse
import json
import math
import subprocess
import sys
import time

import ingestao as ing
import saturacao as s
from config import HOST_CARGA
from execucao import Execucao, executar
from medicao import Medicao, subir_cadvisor

PATAMARES = {100_000: "100mil", 500_000: "500mil", 1_000_000: "1milhao"}
TAXA = 1_000_000 / 15   # amostras por segundo, iguais nos três patamares
DURACAO = 720
AQUECIMENTO = 120


def carga_cardinalidade(execucao, series, duracao):
    pasta, ferramenta = execucao.pasta, execucao.ferramenta
    series_por_metrica = series // s.METRICAS
    intervalo = round(series * 1000 / TAXA)
    alvo = series * 1000 / intervalo
    valores_s = max(1, math.ceil(intervalo / 1000))  # um valor novo por envio
    subprocess.run(["docker", "pull", s.geradores.AVALANCHE], check=True, capture_output=True)
    subir_cadvisor(HOST_CARGA)
    with Medicao(pasta / "recursos_gerador.csv", HOST_CARGA), \
            open(pasta / "gerador.log", "w") as registro:
        inicio = time.time()
        processos = []
        try:
            processos = s._iniciar_avalanche(ferramenta, intervalo, registro,
                                             series_por_metrica=series_por_metrica,
                                             valores_s=valores_s)
            instancia_inicio = s._cpu_instancia()
            amostras = ing._amostrar(lambda: s.ler_backend(ferramenta), pasta, duracao, inicio)
            cpu_instancia = s._pct_instancia(instancia_inicio, s._cpu_instancia())
            encerrados = sum(1 for p in processos if p.poll() is not None)
        finally:
            s._remover_geradores()
            for processo in processos:
                try:
                    processo.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    processo.kill()
    # Com envios a cada 15 s, um minuto contém só 4 envios, e a taxa lida nele oscila 25%
    # conforme o alinhamento das leituras. O trecho final passa a ter 20 envios, e a
    # tolerância cobre a oscilação de um envio a mais ou a menos.
    final_s = max(ing.FINAL, 20 * intervalo / 1000)
    resumo = ing._resumir_carga(amostras, alvo, inicio, True, cpu_instancia, encerrados,
                                final_s=final_s, tolerancia=0.9)
    resumo.update({"series": series, "intervalo_ms": intervalo, "valores_s": valores_s})
    return resumo


def executar_cardinalidade(ferramenta, series, repeticao, duracao, experimento):
    if s.PARES.get(ferramenta) != "metricas":
        sys.exit("a cardinalidade só se aplica ao Prometheus e ao VictoriaMetrics")
    if series % s.METRICAS:
        sys.exit(f"o número de séries precisa ser múltiplo de {s.METRICAS}")
    condicao = PATAMARES.get(series, f"{series}series")
    execucao = Execucao(experimento, "metricas", ferramenta, condicao, repeticao,
                        aquecimento_s=AQUECIMENTO)
    if execucao.concluida:
        print(f"[{ferramenta} {condicao} rep{repeticao}] já concluída, pulando", flush=True)
        return None
    print(f"[{ferramenta} {condicao} rep{repeticao}] {series} séries, {round(TAXA)} amostras/s "
          f"por {duracao} s", flush=True)
    carga_resumo = {}

    def carga():
        carga_resumo.update(carga_cardinalidade(execucao, series, duracao))

    try:
        resumo = executar(execucao, carga)
    finally:
        s._remover_geradores()

    # A contagem do backend confirma o patamar: armazenados é o número de séries (seção 4.7)
    resumo["carga"] = carga_resumo
    resumo["series_confirmadas"] = resumo["armazenados"] == series
    with open(execucao.pasta / "resumo.json", "w") as arquivo:
        json.dump(resumo, arquivo, indent=2, ensure_ascii=False)

    c = carga_resumo
    memoria = sum(v["memoria_max_mib"] for v in resumo["recursos_backend"].values())
    cpu = sum(v["cpu_pct"] or 0 for v in resumo["recursos_backend"].values())
    print(f"[{ferramenta} {condicao} rep{repeticao}] entregue {c['entregue']} de {c['alvo']}/s, "
          f"{'sustentada' if c['sustentada'] else 'NÃO sustentada'}, erros {c['erros']}, "
          f"séries guardadas {resumo['armazenados']} "
          f"({'confere' if resumo['series_confirmadas'] else 'NÃO confere'}), "
          f"CPU {round(cpu)}%, memória máxima {round(memoria)} MiB", flush=True)
    return resumo


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ferramenta", choices=["prometheus", "victoriametrics"])
    parser.add_argument("--series", type=int, required=True)
    parser.add_argument("--repeticao", type=int, default=1)
    parser.add_argument("--duracao", type=int, default=DURACAO)
    parser.add_argument("--experimento", default="cardinalidade")
    args = parser.parse_args()
    if args.duracao <= AQUECIMENTO + ing.FINAL:
        sys.exit(f"--duracao precisa passar de {AQUECIMENTO + ing.FINAL} s")
    executar_cardinalidade(args.ferramenta, args.series, args.repeticao, args.duracao,
                           args.experimento)


if __name__ == "__main__":
    main()
