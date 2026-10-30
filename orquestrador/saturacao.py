"""Saturação dos três pares: carga em degraus até o backend não acompanhar.

Uso, a partir da pasta orquestrador:
    python saturacao.py rampa <ferramenta> [--inicio 1000] [--degrau 60]
        Piloto (P1): dobra a taxa a cada degrau, a partir de --inicio itens por segundo.
    python saturacao.py degraus <ferramenta> --estimativa N [--repeticao 1] [--degrau 180]
                                             [--experimento saturacao]
        Experimento: de 50% a 150% da estimativa, com incremento de 10% (seção 4.7).
        No piloto, --experimento piloto separa os resultados dos do experimento.

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

import carga as c
from config import HOST_CARGA, PARES
from execucao import SINAIS, Execucao, executar
from medicao import Medicao, subir_cadvisor

ACOMODACAO = 10     # segundos ignorados no início e no fim de cada degrau
REGIME = 60         # segundos finais do degrau em que as taxas são medidas (P2)
LIMITE_GERADOR = 0.9  # fração da CPU da instância 3 a partir da qual o gerador é o limite (P5)


def _regime(amostras):
    """Primeira e última leitura do trecho final do degrau.

    No P2, degraus perto do limite entregaram bem no primeiro minuto e caíram no terceiro.
    A média do degrau inteiro escondia essa queda, então as taxas usam só o fim.
    """
    final = [a for a in amostras if a[0] >= amostras[-1][0] - REGIME]
    if len(final) < 2:
        final = amostras
    return final[0], final[-1]


def avaliar(amostras, alvo, cpu_instancia=None):
    """Classifica um degrau pelas leituras do Collector feitas durante ele."""
    (_, a), (_, b) = amostras[0], amostras[-1]
    falhas = (b["falhas_envio"] - a["falhas_envio"]) + (b["falhas_fila"] - a["falhas_fila"])
    recusados = b["recusados"] - a["recusados"]
    (t0, ra), (t1, rb) = _regime(amostras)
    aceita = (rb["aceitos"] - ra["aceitos"]) / (t1 - t0)
    enviada = (rb["enviados"] - ra["enviados"]) / (t1 - t0)

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
    if falhas > 0 or fila_cheia or enviada < c.TOLERANCIA * aceita:
        estado = "saturado"            # o backend não acompanhou o que o Collector recebeu
    elif recusados > 0:
        estado = "recusado"            # o Collector recusou dados com o backend em dia
    elif aceita < c.TOLERANCIA * alvo:
        # Abaixo do alvo sem fila cheia nem falhas. O exportador do Elasticsearch bloqueia
        # quando a fila enche, em vez de descartar, e esse bloqueio volta até o gerador,
        # que passa a enviar menos. Só conta como limite do gerador com a instância 3 cheia.
        if cpu_instancia is None or cpu_instancia >= 100 * LIMITE_GERADOR:
            estado = "gerador_limitado"    # a taxa pedida não foi atingida (P5)
        else:
            estado = "saturado"            # o backend segurou o Collector, que segurou o gerador
    else:
        estado = "ok"
    return {
        "alvo": round(alvo), "aceita": round(aceita), "enviada": round(enviada),
        "falhas": int(falhas), "recusados": int(recusados),
        "fila_inicio": round(filas[0]), "fila_fim": round(filas[-1]),
        "capacidade_fila": round(capacidade),
        "cpu_instancia3_pct": round(cpu_instancia) if cpu_instancia is not None else None,
        "estado": estado,
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
            processos = c.iniciar_telemetrygen(par, taxa, duracao, registro)
            time.sleep(ACOMODACAO)
            instancia_inicio = c.cpu_instancia()
            amostras = []
            fim = inicio + duracao - ACOMODACAO
            while time.time() < fim:
                leitura = (time.time(), c.ler_collector(sinal))
                amostras.append(leitura)
                c.registrar_amostra(pasta, numero, *leitura)
                time.sleep(c.AMOSTRAGEM)
            cpu_instancia = c.pct_instancia(instancia_inicio, c.cpu_instancia())
            limite = inicio + duracao + 60
            try:
                for processo in processos:
                    processo.wait(timeout=max(1, limite - time.time()))
            except subprocess.TimeoutExpired:
                c.remover_geradores()
                raise RuntimeError(f"os geradores não terminaram no degrau {numero}, "
                                   f"ver gerador_degrau{numero}.log")
            finally:
                registro.close()
            resultado = {"degrau": numero, "inicio": round(inicio, 3), "fim": round(time.time(), 3),
                         **avaliar(amostras, taxa, cpu_instancia)}
            degraus.append(resultado)
            if saida is None:
                saida = csv.DictWriter(arquivo, fieldnames=list(resultado))
                saida.writeheader()
            saida.writerow(resultado)
            arquivo.flush()
            print(f"    aceita {resultado['aceita']}, enviada {resultado['enviada']}, "
                  f"fila {resultado['fila_inicio']} para {resultado['fila_fim']}, "
                  f"instância 3 {resultado['cpu_instancia3_pct']}%: "
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

def avaliar_metricas(amostras, alvo, cpu_gerador, cpu_instancia, encerrados):
    """Classifica um degrau pelas leituras do backend feitas durante ele.

    O avalanche espera cada envio terminar antes do próximo, então um backend lento reduz a
    taxa em vez de acumular fila. Quando a taxa fica abaixo do alvo, a CPU do gerador separa
    os dois casos: perto do limite da instância 3, quem não acompanhou foi o gerador. A
    medida vale tanto pelos contêineres do avalanche quanto pela instância inteira, que inclui
    o tráfego de rede tratado pelo sistema.
    """
    (_, a), (_, b) = amostras[0], amostras[-1]
    (t0, ra), (t1, rb) = _regime(amostras)
    aceita = (rb["aceitas"] - ra["aceitas"]) / (t1 - t0)
    gravada = (rb["gravadas"] - ra["gravadas"]) / (t1 - t0)
    erros = int(b["erros"] - a["erros"]) + encerrados
    esperas = int(b["esperas"] - a["esperas"])
    limite = LIMITE_GERADOR * 100 * os.cpu_count()
    gerador_no_limite = ((cpu_gerador is not None and cpu_gerador >= limite)
                         or (cpu_instancia is not None and cpu_instancia >= 100 * LIMITE_GERADOR))

    if erros > 0 or gravada < c.TOLERANCIA * aceita:
        estado = "saturado"            # o backend recusou envios ou não gravou o que recebeu
    elif aceita < c.TOLERANCIA * alvo:
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
    c.baixar_avalanche()
    degraus = []
    with open(pasta / "degraus.csv", "w", newline="") as arquivo:
        saida = None
        for numero, taxa in enumerate(taxas, start=1):
            intervalo = c.intervalo_ms(taxa)
            alvo = c.SERIES * 1000 / intervalo
            print(f"  degrau {numero}: {round(alvo)} amostras/s por {duracao} s "
                  f"({c.SERIES} séries a cada {intervalo} ms)", flush=True)
            inicio = time.time()
            registro = open(pasta / f"gerador_degrau{numero}.log", "w")
            processos = []
            try:
                processos = c.iniciar_avalanche(ferramenta, intervalo, registro)
                time.sleep(ACOMODACAO)
                cpu_inicio = (time.time(), c.cpu_geradores())
                instancia_inicio = c.cpu_instancia()
                amostras = []
                fim = inicio + duracao - ACOMODACAO
                while time.time() < fim:
                    leitura = (time.time(), c.ler_backend(ferramenta))
                    amostras.append(leitura)
                    c.registrar_amostra(pasta, numero, *leitura)
                    time.sleep(c.AMOSTRAGEM)
                cpu_fim = (time.time(), c.cpu_geradores())
                instancia_fim = c.cpu_instancia()
                # um avalanche que terminou sozinho desistiu depois de mais de 20 erros de envio
                encerrados = sum(1 for p in processos if p.poll() is not None)
            finally:
                c.remover_geradores()
                c.encerrar(processos)
                registro.close()
            cpu = None
            if cpu_inicio[1] is not None and cpu_fim[1] is not None:
                cpu = (cpu_fim[1] - cpu_inicio[1]) / (cpu_fim[0] - cpu_inicio[0]) * 100
            cpu_instancia = c.pct_instancia(instancia_inicio, instancia_fim)
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

    def completar(resumo):
        resumo["saturacao"] = conclusao   # a mesma conclusão do saturacao.json

    try:
        executar(execucao, carga, completar=completar)
    finally:
        c.remover_geradores()
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
    parser.add_argument("--experimento", default="saturacao",
                        help="pasta de resultados do modo degraus, por exemplo piloto")
    parser.add_argument("--geradores", type=int, default=8,
                        help="contêineres do avalanche no par de métricas")
    args = parser.parse_args()
    c.configurar_geradores(args.geradores)
    sufixo = f"-g{args.geradores}" if args.geradores != 8 else ""

    if args.modo == "rampa":
        taxas = [args.inicio * 2 ** k for k in range(12)]  # até 2048 vezes o início
        executar_saturacao(args.ferramenta, "rampa", f"inicio{round(args.inicio)}{sufixo}",
                           args.repeticao, taxas, args.degrau or 60)
    else:
        if not args.estimativa:
            sys.exit("informe --estimativa, a vazão estimada na rampa")
        taxas = [args.estimativa * f / 10 for f in range(5, 16)]  # 50% a 150%
        executar_saturacao(args.ferramenta, args.experimento, f"degraus{sufixo}", args.repeticao,
                           taxas, args.degrau or 180)


if __name__ == "__main__":
    main()
