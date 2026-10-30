"""Consulta: volume fixo ingerido e consultas repetidas pelo Locust, nos pares de logs e traces.

Mede o tempo de resposta das consultas típicas de cada sinal sobre o mesmo volume de dados.

Logs: 1 milhão de linhas do flog, geradas uma vez na instância 2 e reaproveitadas, em 100
arquivos que viram 100 serviços. O filtro por rótulo pega um serviço (1%) e a busca textual
pega um termo presente em 1% das linhas. Traces: 100 serviços com 1000 traces de 5 spans
cada, e 10 traces a mais com o atributo tcc.busca=alvo no serviço consultado. A busca por
serviço pega 1% dos traces, e a busca por atributo, 1% dos traces desse serviço.

Depois da ingestão, o backend grava tudo e termina as fusões em andamento, para que as
consultas não disputem recursos com a indexação. Um único usuário do Locust repete as
consultas num aquecimento não medido, até os tempos estabilizarem (consultas/aquecimento.py),
e depois mais 30 vezes. Os tempos medidos ficam em tempos.csv na pasta da execução, e o resumo
traz a variação entre as 10 primeiras e as 10 últimas repetições de cada consulta, para
conferir que o aquecimento bastou.

Uso, a partir da pasta orquestrador:
    python consultas.py <loki|elasticsearch|jaeger|zipkin> [--repeticao 1] [--experimento consulta]
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
from config import HOST_DEMO, NA_AWS, PARES, RAIZ, VERSOES, docker_em, hosts_extras, na_instancia2
from execucao import FERRAMENTAS, Execucao, executar

# o experimento de consulta cobre só os pares de logs e de traces
CONSULTADAS = {f: p for f, p in PARES.items() if p != "metricas"}
# nos logs, o Collector lê os arquivos do flog em vez de receber OTLP
CONFIG_COLLECTOR = {"loki": "consulta-loki", "elasticsearch": "consulta-es"}
SERVICOS = 100
SERVICO = "svc-007"           # o serviço consultado, 1% dos dados
TERMO = "marcadortcc"         # presente em 1% das linhas de logs
LINHAS = 1_000_000            # linhas de logs no total
TRACES_POR_SERVICO = 1000
MARCADOS = 10                 # traces com tcc.busca=alvo no serviço consultado
SPANS_POR_TRACE = 1 + formato.FILHOS
TAXA_TRACES = 800             # spans/s por gerador, 10 em paralelo, cerca de metade do S de traces
PARALELOS = 10                # geradores de traces rodando ao mesmo tempo
LIMITE_LOGS = 1000            # linhas por consulta, o padrão do Grafana para o Loki, nos dois
LIMITE_TRACES = 200           # traces por consulta
REPETICOES = 30               # repetições medidas de cada consulta, depois do aquecimento
DADOS = "/home/ubuntu/dados-consulta/logs" if NA_AWS else str(Path.home() / "dados-consulta/logs")


# ---------------------------------------------------------------- ingestão

def garantir_logs():
    """Gera os arquivos fixos de logs na instância 2, se ainda não existirem.

    O flog gera as linhas, e o script gerar_logs_consulta.py as divide em 100 arquivos e
    marca 1% delas com o termo buscado. O arquivo pronto indica que a geração terminou, e
    assim todas as execuções consultam exatamente os mesmos dados.
    """
    na_instancia2(
        f"test -f {DADOS}/pronto || (docker run --rm {geradores.FLOG} -f apache_combined "
        f"-n {LINHAS} -t stdout | python3 scripts/gerar_logs_consulta.py {DADOS} {SERVICOS} {TERMO})")


def copiar_logs():
    """Copia os arquivos para o volume que o Collector lê, o que dispara a ingestão.

    A cópia roda num contêiner alpine na instância 2, que monta a pasta dos dados e o volume.
    """
    subprocess.run(["docker", "run", "--rm", "-v", "flog-logs:/d", "-v", f"{DADOS}:/s:ro",
                    "alpine", "sh", "-c", "cp /s/*.log /d/"],
                   env=docker_em(HOST_DEMO), check=True, capture_output=True)


def _telemetrygen_traces(servico, traces, marcado):
    """Comando docker de um telemetrygen que envia traces de um serviço.

    Os traces têm o formato dos sintéticos. Com marcado, cada span recebe também o atributo
    tcc.busca=alvo, procurado na busca por atributo.
    """
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
    """Espera o backend guardar todos os itens, por até limite segundos.

    O Loki conta também o que está em memória. Nos outros, a descarga torna contável o que
    acabou de chegar, como o refresh do Elasticsearch.
    """
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
    """Roda o Locust com um usuário e o arquivo de consultas do par, na instância 3.

    Os parâmetros vão por variáveis de ambiente, incluindo o número exato de itens que cada
    consulta deve devolver, e a pasta da execução é montada para receber os tempos. O Locust
    termina sozinho depois das repetições, e o tempo de 30 minutos é só um limite de segurança.
    """
    par = CONSULTADAS[ferramenta]
    variaveis = {"ALVO": ferramenta, "SERVICO": SERVICO, "REPETICOES": REPETICOES}
    if par == "logs":
        variaveis.update(TERMO=TERMO, ESPERADO=LIMITE_LOGS)
    else:
        # a busca por serviço devolve o limite de traces, e a por atributo, os marcados
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


def _postos(valores):
    """Posição de cada valor na ordem crescente, de 0 em diante."""
    ordem = sorted(range(len(valores)), key=lambda i: valores[i])
    postos = [0] * len(valores)
    for posto, i in enumerate(ordem):
        postos[i] = posto
    return postos


def _spearman(valores):
    """Correlação de Spearman entre a ordem das repetições e os tempos.

    Perto de -1, os tempos caem ao longo das repetições, sinal de aquecimento ainda em curso.
    Perto de 0, não há tendência. Usa a fórmula sem correção para empates, que são raros em
    tempos medidos em milissegundos com casas decimais.
    """
    n = len(valores)
    if n < 3:
        return None
    soma = sum((i - p) ** 2 for i, p in enumerate(_postos(valores)))
    return round(1 - 6 * soma / (n * (n * n - 1)), 3)


def resumir_tempos(arquivo):
    """Mediana, percentil 95, média, falhas e tendência de cada consulta, em milissegundos.

    A variação compara as medianas das 10 primeiras e das 10 últimas repetições. Até 5% para
    mais ou para menos, o aquecimento bastou e os tempos medidos estão estáveis.
    """
    if not arquivo.exists():
        return {}
    por = {}
    with open(arquivo) as f:
        for l in csv.DictReader(f):
            por.setdefault(l["consulta"], []).append(l)
    resumo = {}
    for consulta, ls in por.items():
        na_ordem = [float(l["ms"]) for l in sorted(ls, key=lambda l: float(l["inicio"]))]
        tempos = sorted(na_ordem)
        inicio, fim = statistics.median(na_ordem[:10]), statistics.median(na_ordem[-10:])
        resumo[consulta] = {
            "repeticoes": len(ls),
            "falhas": sum(1 for l in ls if l["erro"]),
            "mediana_ms": round(statistics.median(tempos), 1),
            "p95_ms": round(tempos[max(0, round(0.95 * len(tempos)) - 1)], 1),
            "media_ms": round(statistics.mean(tempos), 1),
            # variação entre as 10 primeiras e as 10 últimas repetições medidas
            "variacao_pct": round(100 * (fim / inicio - 1), 1) if inicio else None,
            "spearman": _spearman(na_ordem),
        }
    return resumo


# ---------------------------------------------------------------- execução

def executar_consulta(ferramenta, repeticao, experimento):
    """Uma execução completa: ingestão do volume fixo, aquecimento e consultas medidas."""
    par = CONSULTADAS[ferramenta]
    execucao = Execucao(experimento, par, ferramenta, "fixo", repeticao, aquecimento_s=0)
    if execucao.concluida:
        print(f"[{ferramenta} fixo rep{repeticao}] já concluída, pulando", flush=True)
        return None
    classe, _ = FERRAMENTAS[ferramenta]
    esperado = (LINHAS if par == "logs"
                else (SERVICOS * TRACES_POR_SERVICO + MARCADOS) * SPANS_POR_TRACE)
    tempos = {}

    def carga():
        """Ingere o volume fixo, espera o backend guardar e acomodar tudo e roda o Locust."""
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

    def completar(resumo):
        """Acrescenta ao resumo os tempos de ingestão, as consultas e o aquecimento."""
        resumo.update(tempos)
        consultas = resumir_tempos(execucao.pasta / "tempos.csv")
        if not consultas:
            # sem tempos, a execução não conta como concluída e o rodar.py a refaz
            raise RuntimeError("o Locust não registrou nenhum tempo, ver locust.log")
        resumo["consultas"] = consultas
        aquecimento = execucao.pasta / "aquecimento.json"
        if aquecimento.exists():
            dados = json.loads(aquecimento.read_text())
            resumo["aquecimento"] = {k: dados[k] for k in ("repeticoes", "incompleto")}

    resumo = executar(execucao, carga, config_collector=CONFIG_COLLECTOR.get(ferramenta),
                      completar=completar)
    if "aquecimento" in resumo:
        a = resumo["aquecimento"]
        print(f"[{ferramenta} fixo rep{repeticao}] aquecimento de {a['repeticoes']} "
              f"repetições{', INCOMPLETO' if a['incompleto'] else ''}", flush=True)
    for nome, c in resumo["consultas"].items():
        print(f"[{ferramenta} fixo rep{repeticao}] {nome}: {c['repeticoes']} repetições, "
              f"{c['falhas']} falhas, mediana {c['mediana_ms']} ms, p95 {c['p95_ms']} ms, "
              f"variação {c['variacao_pct']}%", flush=True)
    return resumo


def main():
    """Lê os argumentos e roda uma execução de consulta."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ferramenta", choices=list(CONSULTADAS))
    parser.add_argument("--repeticao", type=int, default=1)
    parser.add_argument("--experimento", default="consulta")
    args = parser.parse_args()
    executar_consulta(args.ferramenta, args.repeticao, args.experimento)


if __name__ == "__main__":
    main()
