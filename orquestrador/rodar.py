"""Experimento completo em blocos aleatorizados, com retomada (seção 4.8).

Uso, a partir da pasta orquestrador:
    python rodar.py [--experimentos ingestao realista ...] [--blocos 1 2 ...] [--horas N]
                    [--desligar] [--plano]

Cada bloco é uma repetição e contém as 40 combinações de experimento, ferramenta e condição,
em ordem sorteada dentro do bloco. Os 5 blocos somam 200 execuções. A ordem é sorteada uma
única vez, com semente fixa, e gravada em resultados/plano.json, para que uma retomada siga
a mesma sequência.

Cada execução roda como um processo separado, chamando o script do experimento. Uma
execução com resumo.json é considerada concluída e pulada, então basta rodar o comando de
novo para continuar de onde parou. A pasta de uma execução incompleta, interrompida ou com
falha, é movida para resultados/_falhas antes de a execução ser refeita, porque alguns
arquivos são gravados acumulando e misturariam as duas tentativas. Uma execução em que um
backend esgota a memória também é descartada e refeita (A24), exceto na saturação, em que
isso acontece no degrau que satura e fica só registrado no resumo.

Opções:
    --experimentos  roda só os experimentos indicados, na ordem do plano
    --blocos        roda só os blocos indicados
    --horas         não inicia nenhuma execução depois de N horas, para dividir o
                    experimento em dias. A execução em andamento termina normalmente
    --desligar      ao final, desliga as instâncias 1 e 2 e depois a própria instância 3
    --plano         mostra o plano, o que já foi feito e o tempo estimado, sem executar
"""
import argparse
import gzip
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from config import HOST_BACKEND, HOST_DEMO, NA_AWS, RAIZ, docker_em

RESULTADOS = RAIZ / "resultados"
PLANO = RESULTADOS / "plano.json"
FALHAS = RESULTADOS / "_falhas"
REGISTROS = RESULTADOS / "_rodar"
ORQUESTRADOR = Path(__file__).resolve().parent

BLOCOS = 5
SEMENTE = 20261030
TENTATIVAS = 3             # por execução, em cada chamada do rodar.py
USUARIOS = 50              # cenário realista (P4)
INTERRUPCAO = 120          # segundos para a execução encerrar depois do Ctrl+C simulado
PAUSA = 60                 # segundos entre uma tentativa com problema e a seguinte

PARES = {"loki": "logs", "elasticsearch": "logs", "jaeger": "traces", "zipkin": "traces",
         "prometheus": "metricas", "victoriametrics": "metricas"}
NIVEIS = (25, 50, 75)
SERIES = {100_000: "100mil", 500_000: "500mil", 1_000_000: "1milhao"}

# Centro dos degraus da saturação, que vão de 50% a 150% deste valor. Elasticsearch, Jaeger
# e Prometheus vêm do piloto em degraus (A26). Loki e Zipkin ficam no meio das faixas das
# rampas (A22 e A24). O VictoriaMetrics fica no limite da instância 3 e para no degrau
# classificado como gerador_limitado, o que dá um limite inferior da capacidade (P5).
ESTIMATIVAS = {"elasticsearch": 58_500, "loki": 120_000, "jaeger": 16_500,
               "zipkin": 48_000, "prometheus": 638_000, "victoriametrics": 2_100_000}

# Minutos por execução, medidos nos pilotos com o custo fixo de cada uma (P11)
MINUTOS = {"saturacao": 30, "ingestao": 15.5, "cardinalidade": 12.5, "realista": 17.5,
           "consulta": 2}
EXPERIMENTOS = list(MINUTOS)


# ---------------------------------------------------------------- plano

def combinacoes():
    """As 40 combinações de um bloco, cada uma com a pasta e o comando que a executa."""
    lista = []
    for f in PARES:
        lista.append({"experimento": "saturacao", "ferramenta": f, "condicao": "degraus"})
    for f in PARES:
        for nivel in NIVEIS:
            lista.append({"experimento": "ingestao", "ferramenta": f, "condicao": f"{nivel}pct"})
    for f in ("prometheus", "victoriametrics"):
        for nome in SERIES.values():
            lista.append({"experimento": "cardinalidade", "ferramenta": f, "condicao": nome})
    for f in PARES:
        lista.append({"experimento": "realista", "ferramenta": f,
                      "condicao": f"{USUARIOS}usuarios"})
    for f in ("loki", "elasticsearch", "jaeger", "zipkin"):
        lista.append({"experimento": "consulta", "ferramenta": f, "condicao": "fixo"})
    return lista


def criar_plano(semente):
    sorteio = random.Random(semente)
    execucoes = []
    for bloco in range(1, BLOCOS + 1):
        ordem = combinacoes()
        sorteio.shuffle(ordem)
        execucoes += [{**c, "repeticao": bloco} for c in ordem]
    return {"semente": semente, "criado": datetime.now().isoformat(timespec="seconds"),
            "execucoes": execucoes}


def carregar_plano():
    if PLANO.exists():
        return json.loads(PLANO.read_text())
    plano = criar_plano(SEMENTE)
    RESULTADOS.mkdir(exist_ok=True)
    PLANO.write_text(json.dumps(plano, indent=1, ensure_ascii=False))
    return plano


def pasta(e):
    return (RESULTADOS / e["experimento"] / PARES[e["ferramenta"]] / e["ferramenta"]
            / e["condicao"] / f"rep{e['repeticao']}")


def concluida(e):
    return (pasta(e) / "resumo.json").exists()


def comando(e):
    f, r, x = e["ferramenta"], str(e["repeticao"]), e["experimento"]
    if x == "saturacao":
        return ["saturacao.py", "degraus", f, "--estimativa", str(ESTIMATIVAS[f]),
                "--repeticao", r, "--experimento", x]
    if x == "ingestao":
        return ["ingestao.py", f, "--nivel", e["condicao"].removesuffix("pct"),
                "--repeticao", r, "--experimento", x]
    if x == "cardinalidade":
        series = next(n for n, nome in SERIES.items() if nome == e["condicao"])
        return ["cardinalidade.py", f, "--series", str(series), "--repeticao", r,
                "--experimento", x]
    if x == "realista":
        return ["realista.py", "execucao", f, "--usuarios", str(USUARIOS), "--repeticao", r,
                "--experimento", x]
    return ["consultas.py", f, "--repeticao", r, "--experimento", x]


def nome(e):
    return f"{e['experimento']} {e['ferramenta']} {e['condicao']} rep{e['repeticao']}"


# ---------------------------------------------------------------- execução

class Registro:
    """Mostra na tela e grava no registro da sessão ao mesmo tempo."""

    def __init__(self):
        REGISTROS.mkdir(parents=True, exist_ok=True)
        self.arquivo = open(REGISTROS / f"{datetime.now():%Y%m%d-%H%M%S}.log", "a")

    def escrever(self, texto):
        sys.stdout.write(texto)
        sys.stdout.flush()
        self.arquivo.write(texto)
        self.arquivo.flush()

    def linha(self, texto):
        self.escrever(f"{datetime.now():%d/%m %H:%M:%S} {texto}\n")


def historico(e, tentativa, situacao, inicio):
    linha = {"inicio": datetime.fromtimestamp(inicio).isoformat(timespec="seconds"),
             "minutos": round((time.time() - inicio) / 60, 1), "tentativa": tentativa,
             "situacao": situacao, **e}
    with open(REGISTROS / "historico.jsonl", "a") as arquivo:
        arquivo.write(json.dumps(linha, ensure_ascii=False) + "\n")


def arquivar(e, motivo, registro):
    """Move a pasta de uma tentativa descartada para resultados/_falhas."""
    origem = pasta(e)
    if not origem.exists():
        return
    destino = FALHAS / origem.relative_to(RESULTADOS).parent / (
        f"{origem.name}-{motivo}-{datetime.now():%Y%m%d-%H%M%S}")
    destino.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(origem), str(destino))
    registro.linha(f"  pasta anterior movida para {destino.relative_to(RESULTADOS)}")


def rodar_processo(e, registro):
    """Roda o script da execução e devolve False se ele passou do tempo limite."""
    limite = max(3600, 3 * MINUTOS[e["experimento"]] * 60)
    processo = subprocess.Popen([sys.executable, "-u", *comando(e)], cwd=ORQUESTRADOR,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                errors="replace", start_new_session=True)

    def copiar_saida():
        for linha in processo.stdout:
            registro.escrever("    " + linha)

    leitor = threading.Thread(target=copiar_saida, daemon=True)
    leitor.start()

    def interromper():
        # o Ctrl+C deixa os blocos finally pararem geradores, usuários e a Demo
        os.killpg(processo.pid, signal.SIGINT)
        try:
            processo.wait(timeout=INTERRUPCAO)
        except subprocess.TimeoutExpired:
            os.killpg(processo.pid, signal.SIGKILL)
            processo.wait()
        leitor.join(timeout=10)

    try:
        processo.wait(timeout=limite)
    except subprocess.TimeoutExpired:
        registro.linha(f"  passou de {limite // 60} min, interrompendo")
        interromper()
        return False
    except KeyboardInterrupt:
        # o processo roda numa sessão própria e não recebe o Ctrl+C do terminal
        registro.linha("  Ctrl+C recebido, encerrando a execução em andamento")
        interromper()
        raise
    leitor.join(timeout=10)
    return True


def guardar_conteineres(e):
    """Guarda os logs dos contêineres da instância 1 e diz se algum esgotou a memória."""
    if not NA_AWS:
        return []
    ambiente = docker_em(HOST_BACKEND)
    nomes = subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}"], env=ambiente,
                           capture_output=True, text=True).stdout.split()
    destino = pasta(e) / "conteineres"
    destino.mkdir(exist_ok=True)
    sem_memoria = []
    for conteiner in nomes:
        if conteiner.startswith("cadvisor"):
            continue
        saida = subprocess.run(["docker", "logs", "--tail", "20000", conteiner], env=ambiente,
                               capture_output=True, text=True, errors="replace")
        texto = saida.stdout + saida.stderr
        with gzip.open(destino / f"{conteiner}.log.gz", "wt") as arquivo:
            arquivo.write(texto)
        morto = subprocess.run(["docker", "inspect", "-f", "{{.State.OOMKilled}}", conteiner],
                               env=ambiente, capture_output=True, text=True).stdout.strip()
        if "java.lang.OutOfMemoryError" in texto or morto == "true":
            sem_memoria.append(conteiner)
    return sem_memoria


def marcar_memoria(e, conteineres):
    arquivo = pasta(e) / "resumo.json"
    resumo = json.loads(arquivo.read_text())
    resumo["memoria_esgotada"] = conteineres
    arquivo.write_text(json.dumps(resumo, indent=2, ensure_ascii=False))


def executar_uma(e, registro):
    """Até TENTATIVAS tentativas. Devolve a situação final."""
    for tentativa in range(1, TENTATIVAS + 1):
        if pasta(e).exists():
            arquivar(e, "incompleta", registro)
        registro.linha(f"> {nome(e)} (tentativa {tentativa})")
        inicio = time.time()
        no_prazo = rodar_processo(e, registro)
        if not concluida(e):
            situacao = "falha" if no_prazo else "tempo_esgotado"
            registro.linha(f"  {situacao}, sem resumo.json")
            historico(e, tentativa, situacao, inicio)
            arquivar(e, situacao, registro)
            time.sleep(PAUSA)
            continue
        sem_memoria = guardar_conteineres(e)
        if sem_memoria and e["experimento"] == "saturacao":
            # Na saturação, esgotar a memória é um modo de saturar (A24). Os degraus param no
            # primeiro que não é ok, e a vazão máxima vem do anterior, então a execução vale.
            registro.linha(f"  memória esgotada em {', '.join(sem_memoria)} no degrau final, "
                           f"registrada no resumo")
            marcar_memoria(e, sem_memoria)
            sem_memoria = []
        if sem_memoria:
            ultima = tentativa == TENTATIVAS
            registro.linha(f"  memória esgotada em {', '.join(sem_memoria)}"
                           + (", mantida por ser a última tentativa" if ultima else ", refazendo"))
            historico(e, tentativa, "sem_memoria", inicio)
            if not ultima:
                arquivar(e, "sem_memoria", registro)
                time.sleep(PAUSA)
                continue
            marcar_memoria(e, sem_memoria)
            return "sem_memoria"
        registro.linha(f"  concluída em {(time.time() - inicio) / 60:.1f} min")
        historico(e, tentativa, "concluida", inicio)
        return "concluida"
    return "falha"


def desligar(registro):
    registro.linha("desligando as instâncias 1, 2 e 3")
    for host in (HOST_BACKEND, HOST_DEMO):
        subprocess.run(["ssh", f"ubuntu@{host}", "sudo shutdown -h now"], capture_output=True)
    subprocess.run(["sudo", "shutdown", "-h", "+1"])


# ---------------------------------------------------------------- principal

def mostrar_plano(execucoes):
    feitas = [e for e in execucoes if concluida(e)]
    faltam = [e for e in execucoes if not concluida(e)]
    print(f"{len(feitas)} de {len(execucoes)} execuções concluídas")
    for x in EXPERIMENTOS:
        total = sum(1 for e in execucoes if e["experimento"] == x)
        if total:
            pendentes = sum(1 for e in faltam if e["experimento"] == x)
            print(f"  {x:<14} {total - pendentes:>3} de {total:>3}, faltam "
                  f"{pendentes * MINUTOS[x] / 60:5.1f} h")
    horas = sum(MINUTOS[e["experimento"]] for e in faltam) / 60
    print(f"tempo estimado para o restante: {horas:.1f} h")
    print("\npróximas execuções:")
    for e in faltam[:10]:
        print(f"  {nome(e)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--experimentos", nargs="+", choices=EXPERIMENTOS)
    parser.add_argument("--blocos", nargs="+", type=int, choices=range(1, BLOCOS + 1))
    parser.add_argument("--horas", type=float)
    parser.add_argument("--desligar", action="store_true")
    parser.add_argument("--plano", action="store_true")
    args = parser.parse_args()

    plano = carregar_plano()
    execucoes = [e for e in plano["execucoes"]
                 if (not args.experimentos or e["experimento"] in args.experimentos)
                 and (not args.blocos or e["repeticao"] in args.blocos)]
    if args.plano:
        mostrar_plano(execucoes)
        return

    registro = Registro()
    limite = time.time() + args.horas * 3600 if args.horas else None
    pendentes = [e for e in execucoes if not concluida(e)]
    registro.linha(f"início: {len(pendentes)} execuções pendentes de {len(execucoes)}, "
                   f"semente {plano['semente']}")
    resultado = {}
    try:
        for numero, e in enumerate(pendentes, start=1):
            if limite and time.time() > limite:
                registro.linha(f"limite de {args.horas} h atingido, parando antes de {nome(e)}")
                break
            restante = sum(MINUTOS[x["experimento"]] for x in pendentes[numero - 1:]) / 60
            registro.linha(f"[{numero}/{len(pendentes)}] faltam cerca de {restante:.1f} h")
            resultado[nome(e)] = executar_uma(e, registro)
    except KeyboardInterrupt:
        registro.linha("interrompido. Rode o mesmo comando para continuar")
        sys.exit(130)
    problemas = {k: v for k, v in resultado.items() if v != "concluida"}
    registro.linha(f"fim: {len(resultado) - len(problemas)} concluídas nesta sessão")
    for chave, situacao in problemas.items():
        registro.linha(f"  atenção: {chave}: {situacao}")
    if args.desligar:
        desligar(registro)


if __name__ == "__main__":
    main()
