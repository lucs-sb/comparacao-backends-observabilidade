"""Ingestão: carga constante em 25%, 50% ou 75% de S, a menor vazão máxima do par (seção 4.7).

Uso, a partir da pasta orquestrador:
    python ingestao.py <ferramenta> --nivel 75 [--repeticao 1] [--duracao 720]
                                    [--experimento ingestao]

A carga dura 12 minutos, e os 2 primeiros são descartados como aquecimento no resumo de CPU e
memória. O disco não é medido aqui, só no cenário realista (A27). Os geradores são os mesmos da saturação, com o mesmo formato de itens (A17) e, nas
métricas, as mesmas 100 mil séries (A25). Uma execução já concluída é pulada, o que permite
retomar uma sequência interrompida.
"""
import argparse
import json
import subprocess
import sys
import time

import requests

import saturacao as s
from config import HOST_CARGA
from execucao import FERRAMENTAS, SINAIS, Execucao, executar
from medicao import Medicao, subir_cadvisor

# S de cada par, medido nos degraus do piloto (A26)
S = {"logs": 58500, "traces": 16500, "metricas": 638000}
NIVEIS = (25, 50, 75)
DURACAO = 720       # 12 minutos de carga
AQUECIMENTO = 120   # 2 minutos descartados
FINAL = 60          # o último minuto mostra se a taxa se sustentou até o fim


def _taxa(amostras, chave, desde):
    """Taxa média de um contador acumulado a partir de um instante."""
    trecho = [a for a in amostras if a[0] >= desde]
    if len(trecho) < 2:
        return None
    (t0, a), (t1, b) = trecho[0], trecho[-1]
    return (b[chave] - a[chave]) / (t1 - t0)


def _resumir_carga(amostras, alvo, inicio, metricas, cpu_instancia, encerrados=0):
    """Taxas depois do aquecimento e no último minuto, e se a carga se sustentou."""
    aceita, entregue = ("aceitas", "gravadas") if metricas else ("aceitos", "enviados")
    desde = inicio + AQUECIMENTO
    final = amostras[-1][0] - FINAL
    (_, a), (_, b) = amostras[0], amostras[-1]
    resumo = {
        "alvo": round(alvo),
        "aceita": round(_taxa(amostras, aceita, desde) or 0),
        "entregue": round(_taxa(amostras, entregue, desde) or 0),
        "entregue_ultimo_minuto": round(_taxa(amostras, entregue, final) or 0),
        "itens_entregues": round(b[entregue] - a[entregue]),
        "cpu_instancia3_pct": round(cpu_instancia) if cpu_instancia is not None else None,
    }
    if metricas:
        resumo["erros"] = int(b["erros"] - a["erros"]) + encerrados
        resumo["esperas_insercao"] = int(b["esperas"] - a["esperas"])
        falhou = resumo["erros"] > 0
    else:
        resumo["falhas"] = int((b["falhas_envio"] - a["falhas_envio"])
                               + (b["falhas_fila"] - a["falhas_fila"]))
        resumo["recusados"] = int(b["recusados"] - a["recusados"])
        resumo["fila_max"] = round(max(v["fila"] for _, v in amostras))
        resumo["capacidade_fila"] = round(b["capacidade_fila"])
        falhou = resumo["falhas"] > 0 or resumo["recusados"] > 0
    # Sustentada: o backend recebeu pelo menos 95% do alvo depois do aquecimento e também
    # no último minuto, sem falhas. Se não, o nível escolhido passou do que ele aguenta.
    resumo["sustentada"] = (not falhou
                            and resumo["entregue"] >= s.TOLERANCIA * alvo
                            and resumo["entregue_ultimo_minuto"] >= s.TOLERANCIA * alvo)
    return resumo


def _amostrar(ler, pasta, duracao, inicio):
    """Lê os contadores a cada 5 segundos até o fim da carga."""
    amostras = []
    while time.time() < inicio + duracao:
        try:
            leitura = (time.time(), ler())
        except requests.RequestException:
            time.sleep(s.AMOSTRAGEM)  # uma leitura perdida não interrompe a execução
            continue
        amostras.append(leitura)
        s._registrar_amostra(pasta, 1, *leitura)
        time.sleep(s.AMOSTRAGEM)
    return amostras


def carga_collector(execucao, taxa, duracao):
    """Logs e traces: um telemetrygen por serviço, enviando ao Collector."""
    pasta, par = execucao.pasta, execucao.par
    sinal = SINAIS[par]
    with open(pasta / "gerador.log", "w") as registro:
        inicio = time.time()
        processos = s._iniciar_geradores(par, taxa, duracao, registro)
        instancia_inicio = s._cpu_instancia()
        amostras = _amostrar(lambda: s.ler_collector(sinal), pasta, duracao, inicio)
        cpu_instancia = s._pct_instancia(instancia_inicio, s._cpu_instancia())
        try:
            for processo in processos:
                processo.wait(timeout=max(1, inicio + duracao + 60 - time.time()))
        except subprocess.TimeoutExpired:
            raise RuntimeError("os geradores não terminaram, ver gerador.log")
    return _resumir_carga(amostras, taxa, inicio, False, cpu_instancia)


def carga_metricas(execucao, taxa, duracao):
    """Métricas: os avalanche enviando direto ao backend, com o intervalo que dá a taxa."""
    pasta, ferramenta = execucao.pasta, execucao.ferramenta
    intervalo = s.intervalo_ms(taxa)
    alvo = s.SERIES * 1000 / intervalo
    subprocess.run(["docker", "pull", s.geradores.AVALANCHE], check=True, capture_output=True)
    subir_cadvisor(HOST_CARGA)
    with Medicao(pasta / "recursos_gerador.csv", HOST_CARGA), \
            open(pasta / "gerador.log", "w") as registro:
        inicio = time.time()
        processos = []
        try:
            processos = s._iniciar_avalanche(ferramenta, intervalo, registro)
            instancia_inicio = s._cpu_instancia()
            amostras = _amostrar(lambda: s.ler_backend(ferramenta), pasta, duracao, inicio)
            cpu_instancia = s._pct_instancia(instancia_inicio, s._cpu_instancia())
            encerrados = sum(1 for p in processos if p.poll() is not None)
        finally:
            s._remover_geradores()
            for processo in processos:
                try:
                    processo.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    processo.kill()
    resumo = _resumir_carga(amostras, alvo, inicio, True, cpu_instancia, encerrados)
    resumo["intervalo_ms"] = intervalo
    return resumo


def recebidos_loki(backend):
    """Linhas recebidas pelo distribuidor do Loki, antes da deduplicação (A16)."""
    total = 0.0
    texto = requests.get(backend.url + "/metrics", timeout=10).text
    for linha in texto.splitlines():
        if linha.startswith("loki_distributor_lines_received_total"):
            total += float(linha.rsplit(" ", 1)[1])
    return round(total)


def executar_ingestao(ferramenta, nivel, repeticao, duracao, experimento):
    par = s.PARES[ferramenta]
    taxa = S[par] * nivel / 100
    execucao = Execucao(experimento, par, ferramenta, f"{nivel}pct", repeticao,
                        aquecimento_s=AQUECIMENTO)
    if execucao.concluida:
        print(f"[{ferramenta} {nivel}pct rep{repeticao}] já concluída, pulando", flush=True)
        return None
    print(f"[{ferramenta} {nivel}pct rep{repeticao}] {round(taxa)} itens/s por {duracao} s",
          flush=True)
    carga_resumo = {}

    def carga():
        if par == "metricas":
            carga_resumo.update(carga_metricas(execucao, taxa, duracao))
        else:
            carga_resumo.update(carga_collector(execucao, taxa, duracao))

    try:
        resumo = executar(execucao, carga)
    finally:
        s._remover_geradores()

    # Complementos do resumo, gravados por cima do resumo.json que executar acabou de escrever
    resumo["carga"] = carga_resumo
    backend = FERRAMENTAS[ferramenta][0]()
    if ferramenta == "loki":
        # perda real: o que o Collector enviou e o Loki não recebeu
        # deduplicação: o que o Loki recebeu e guardou uma só vez (A16)
        recebidos = recebidos_loki(backend)
        resumo["recebidos_backend"] = recebidos
        resumo["deduplicados"] = recebidos - resumo["armazenados"]
        if resumo.get("enviados"):
            resumo["perda_real_pct"] = round(
                100 * (resumo["enviados"] - recebidos) / resumo["enviados"], 3)
    # O disco não é medido na ingestão, só no cenário realista (A27)
    with open(execucao.pasta / "resumo.json", "w") as arquivo:
        json.dump(resumo, arquivo, indent=2, ensure_ascii=False)

    c = carga_resumo
    print(f"[{ferramenta} {nivel}pct rep{repeticao}] entregue {c['entregue']} de {c['alvo']}/s, "
          f"último minuto {c['entregue_ultimo_minuto']}/s, "
          f"{'sustentada' if c['sustentada'] else 'NÃO sustentada'}, "
          f"perda {resumo.get('perda_real_pct', resumo.get('perda_pct', 'sem Collector'))}%",
          flush=True)
    return resumo


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ferramenta", choices=list(s.PARES))
    parser.add_argument("--nivel", type=int, choices=NIVEIS, required=True)
    parser.add_argument("--repeticao", type=int, default=1)
    parser.add_argument("--duracao", type=int, default=DURACAO)
    parser.add_argument("--experimento", default="ingestao")
    args = parser.parse_args()
    if args.duracao <= AQUECIMENTO + FINAL:
        sys.exit(f"--duracao precisa passar de {AQUECIMENTO + FINAL} s")
    executar_ingestao(args.ferramenta, args.nivel, args.repeticao, args.duracao,
                      args.experimento)


if __name__ == "__main__":
    main()
