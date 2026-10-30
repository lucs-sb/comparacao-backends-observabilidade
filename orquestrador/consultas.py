"""Consulta: volume fixo ingerido e consultas repetidas pelo Locust (seção 4.7, P7).

Uso, a partir da pasta orquestrador:
    python consultas.py <loki|elasticsearch|jaeger|zipkin> [--repeticao 1] [--experimento consulta]

Logs: 1 milhão de linhas do flog, geradas uma vez na instância 2 e reaproveitadas, em 100
arquivos que viram 100 serviços. O filtro por rótulo pega um serviço (1%) e a busca textual
pega um termo presente em 1% das linhas. Traces: 100 serviços com 1000 traces de 5 spans
cada, e 10 traces a mais com o atributo tcc.busca=alvo no serviço consultado. A busca por
serviço pega 1% dos traces, e a busca por atributo, 1% dos traces desse serviço.

Depois da ingestão, o backend grava tudo e termina as fusões em andamento, para que as
consultas não disputem recursos com a indexação. Cada consulta é repetida 30 vezes por um
único usuário do Locust, e os tempos ficam em tempos.csv na pasta da execução.
"""
import argparse
import csv
import json
import statistics
import subprocess
import time
from pathlib import Path

import formato
import geradores
from config import HOST_DEMO, NA_AWS, RAIZ, VERSOES, docker_em, hosts_extras
from execucao import FERRAMENTAS, Execucao, executar
from realista import _na_instancia2

PARES = {"loki": "logs", "elasticsearch": "logs", "jaeger": "traces", "zipkin": "traces"}
CONFIG_COLLECTOR = {"loki": "consulta-loki", "elasticsearch": "consulta-es"}
SERVICOS = 100
SERVICO = "svc-007"           # o serviço consultado, 1% dos dados
TERMO = "marcadortcc"         # presente em 1% das linhas de logs
LINHAS = 1_000_000
TRACES_POR_SERVICO = 1000
MARCADOS = 10                 # traces com tcc.busca=alvo no serviço consultado
SPANS_POR_TRACE = 1 + formato.FILHOS
TAXA_TRACES = 800             # spans/s por gerador, 10 em paralelo, cerca de metade do S de traces
PARALELOS = 10
LIMITE_LOGS = 1000            # linhas por consulta, o padrão do Grafana para o Loki, nos dois
LIMITE_TRACES = 200
REPETICOES = 30
DADOS = "/home/ubuntu/dados-consulta/logs" if NA_AWS else str(Path.home() / "dados-consulta/logs")


# ---------------------------------------------------------------- ingestão

def garantir_logs():
    """Gera os arquivos fixos de logs na instância 2, se ainda não existirem."""
    _na_instancia2(
        f"test -f {DADOS}/pronto || (docker run --rm {geradores.FLOG} -f apache_combined "
        f"-n {LINHAS} -t stdout | python3 scripts/gerar_logs_consulta.py {DADOS} {SERVICOS} {TERMO})")


def copiar_logs():
    """Copia os arquivos para o volume que o Collector lê, o que dispara a ingestão."""
    subprocess.run(["docker", "run", "--rm", "-v", "flog-logs:/d", "-v", f"{DADOS}:/s:ro",
                    "alpine", "sh", "-c", "cp /s/*.log /d/"],
                   env=docker_em(HOST_DEMO), check=True, capture_output=True)


def _telemetrygen_traces(servico, traces, marcado):
    argumentos = ["traces", "--traces", str(traces), "--workers", "1",
                  "--rate", str(TAXA_TRACES), "--service", servico,
                  "--child-spans", str(formato.FILHOS)]
    for atributo in formato.ATRIBUTOS_RECURSO:
        argumentos += ["--otlp-attributes", atributo]
    for atributo in formato.ATRIBUTOS_SPAN + (['tcc.busca="alvo"'] if marcado else []):
        argumentos += ["--telemetry-attributes", atributo]
    return ["docker", "run", "--rm", "--network", "tcc", *hosts_extras(), geradores.TELEMETRYGEN,
            *argumentos, "--otlp-endpoint", "otelcol:4317", "--otlp-insecure"]


def enviar_traces(registro):
    """100 serviços em lotes de 10 geradores, mais os traces marcados do serviço consultado."""
    comandos = [_telemetrygen_traces(f"svc-{s:03d}", TRACES_POR_SERVICO, False)
                for s in range(SERVICOS)]
    comandos.append(_telemetrygen_traces(SERVICO, MARCADOS, True))
    for i in range(0, len(comandos), PARALELOS):
        processos = [subprocess.Popen(c, stdout=registro, stderr=subprocess.STDOUT)
                     for c in comandos[i:i + PARALELOS]]
        for processo in processos:
            if processo.wait(timeout=600) != 0:
                raise RuntimeError("um gerador de traces falhou, ver gerador.log")


def esperar(backend, ferramenta, esperado, limite=1800):
    """Espera o backend guardar todos os itens. O Loki conta também o que está em memória."""
    fim = time.time() + limite
    while time.time() < fim:
        if ferramenta != "loki":
            backend.descarregar()   # o refresh do Elasticsearch torna os itens contáveis
        if backend.contar() >= esperado:
            return
        time.sleep(30)
    raise TimeoutError(f"o backend não chegou a {esperado} itens em {limite} s")


# ---------------------------------------------------------------- consultas

def rodar_locust(pasta, ferramenta):
    par = PARES[ferramenta]
    variaveis = {"ALVO": ferramenta, "SERVICO": SERVICO, "REPETICOES": REPETICOES}
    if par == "logs":
        variaveis.update(TERMO=TERMO, ESPERADO=LIMITE_LOGS)
    else:
        servico = min(LIMITE_TRACES, TRACES_POR_SERVICO + MARCADOS)
        variaveis.update(ESPERADO_SERVICO=f"{servico},{servico * SPANS_POR_TRACE}",
                         ESPERADO_ATRIBUTO=f"{MARCADOS},{MARCADOS * SPANS_POR_TRACE}")
    ambiente = [x for k, v in variaveis.items() for x in ("-e", f"{k}={v}")]
    with open(pasta / "locust.log", "w") as registro:
        subprocess.run(
            ["docker", "run", "--rm", "--network", "tcc", *hosts_extras(), *ambiente,
             "-v", f"{RAIZ / 'consultas'}:/mnt/locust:ro", "-v", f"{pasta}:/saida",
             f"locustio/locust:{VERSOES['LOCUST_VERSION']}",
             "-f", f"/mnt/locust/{par}.py", "--headless", "-u", "1", "-r", "1",
             "--run-time", "30m", "--only-summary"],
            stdout=registro, stderr=subprocess.STDOUT, check=False)


def resumir_tempos(arquivo):
    """Mediana, percentil 95, média e falhas de cada consulta, em milissegundos."""
    por = {}
    with open(arquivo) as f:
        for l in csv.DictReader(f):
            por.setdefault(l["consulta"], []).append(l)
    resumo = {}
    for consulta, ls in por.items():
        tempos = sorted(float(l["ms"]) for l in ls)
        resumo[consulta] = {
            "repeticoes": len(ls),
            "falhas": sum(1 for l in ls if l["erro"]),
            "mediana_ms": round(statistics.median(tempos), 1),
            "p95_ms": round(tempos[max(0, round(0.95 * len(tempos)) - 1)], 1),
            "media_ms": round(statistics.mean(tempos), 1),
        }
    return resumo


# ---------------------------------------------------------------- execução

def executar_consulta(ferramenta, repeticao, experimento):
    par = PARES[ferramenta]
    execucao = Execucao(experimento, par, ferramenta, "fixo", repeticao, aquecimento_s=0)
    if execucao.concluida:
        print(f"[{ferramenta} fixo rep{repeticao}] já concluída, pulando", flush=True)
        return None
    classe, _ = FERRAMENTAS[ferramenta]
    esperado = (LINHAS if par == "logs"
                else (SERVICOS * TRACES_POR_SERVICO + MARCADOS) * SPANS_POR_TRACE)
    tempos = {}

    def carga():
        backend = classe()
        inicio = time.time()
        if par == "logs":
            garantir_logs()
            copiar_logs()
        else:
            with open(execucao.pasta / "gerador.log", "w") as registro:
                enviar_traces(registro)
        esperar(backend, ferramenta, esperado)
        tempos["ingestao_s"] = round(time.time() - inicio)
        print(f"[{ferramenta} fixo rep{repeticao}] {esperado} itens guardados em "
              f"{tempos['ingestao_s']} s, acomodando e consultando", flush=True)
        backend.descarregar()
        backend.acomodar()          # sem fusões em andamento durante as consultas
        inicio = time.time()
        rodar_locust(execucao.pasta, ferramenta)
        tempos["consultas_s"] = round(time.time() - inicio)

    resumo = executar(execucao, carga, config_collector=CONFIG_COLLECTOR.get(ferramenta))
    resumo.update(tempos)
    arquivo = execucao.pasta / "tempos.csv"
    resumo["consultas"] = resumir_tempos(arquivo) if arquivo.exists() else {}
    with open(execucao.pasta / "resumo.json", "w") as saida:
        json.dump(resumo, saida, indent=2, ensure_ascii=False)
    for nome, c in resumo["consultas"].items():
        print(f"[{ferramenta} fixo rep{repeticao}] {nome}: {c['repeticoes']} repetições, "
              f"{c['falhas']} falhas, mediana {c['mediana_ms']} ms, p95 {c['p95_ms']} ms", flush=True)
    if not resumo["consultas"]:
        print(f"[{ferramenta} fixo rep{repeticao}] sem tempos, ver locust.log", flush=True)
    return resumo


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ferramenta", choices=list(PARES))
    parser.add_argument("--repeticao", type=int, default=1)
    parser.add_argument("--experimento", default="consulta")
    args = parser.parse_args()
    executar_consulta(args.ferramenta, args.repeticao, args.experimento)


if __name__ == "__main__":
    main()
