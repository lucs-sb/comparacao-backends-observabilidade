"""Cenário realista: a OpenTelemetry Demo sob carga do Locust, com cada backend.

A OpenTelemetry Demo é uma loja virtual feita de microsserviços instrumentados. O gerador de
carga dela, o Locust, simula usuários navegando e comprando, e os serviços geram logs, traces
e métricas reais, que o Collector da Demo envia ao backend avaliado. É o único experimento em
que o conteúdo dos dados vem de uma aplicação real, e por isso é nele que o uso de disco é
medido.

Para cada par, um arquivo de extras (demo/extras-<par>.yml) muda o Collector da Demo para
enviar só o sinal avaliado ao backend e descartar os outros. A Demo roda na instância 2 e é
comandada por SSH, porque o Collector dela lê esse arquivo do disco da instância 2. O Locust
começa parado e é ligado pela API dele, para que a carga dure exatamente o tempo da medição.

Uso, a partir da pasta orquestrador:
    python realista.py execucao <ferramenta> --usuarios N [--repeticao 1] [--duracao 720]
                                [--experimento realista]
        Experimento: Demo nova a cada execução, 12 minutos de carga, 2 de aquecimento,
        e o uso de disco medido ao final.
    python realista.py usuarios [--ferramenta loki] [--niveis 25 50 100 200] [--passo 300]
        Piloto: sobe a Demo uma vez e aplica cada número de usuários por --passo segundos,
        mostrando a CPU da instância 2, os erros do Locust e o volume de telemetria, para
        escolher o número de usuários do experimento.
"""
import argparse
import json
import shlex
import sys
import time

import requests

import collector
from config import AMBIENTE, HOST_BACKEND, HOST_DEMO, NA_AWS, PARES, RAIZ, na_instancia2
from execucao import FERRAMENTAS, SINAIS, Execucao, executar
from ingestao import completar_loki
from medicao import ler, subir_cadvisor

# ferramenta: (arquivo de extras do Collector da Demo, exportador do sinal avaliado). O nome do
# exportador limita a contagem de enviados, porque o Collector da Demo tem vários.
EXTRAS = {
    "loki": ("logs-loki", "otlp_http/loki"),
    "elasticsearch": ("logs-es", "elasticsearch"),
    "jaeger": ("traces-jaeger", "otlp_http/jaeger"),
    "zipkin": ("traces-zipkin", "zipkin"),
    "prometheus": ("metricas-prom", "prometheusremotewrite/backend"),
    "victoriametrics": ("metricas-vm", "prometheusremotewrite/backend"),
}
LOCUST = f"http://{HOST_DEMO}:8080/loadgen"   # API do Locust, pelo proxy da Demo
DURACAO = 720       # 12 minutos de carga
AQUECIMENTO = 120   # 2 minutos descartados
LIMITE_CPU_DEMO = 70   # piloto: CPU máxima da instância 2 para um número de usuários valer


# ---------------------------------------------------------------- Demo na instância 2

def _gomemlimit(memoria):
    """80% de um limite como 8g ou 1G, no formato do GOMEMLIMIT."""
    numero, unidade = float(memoria[:-1]), memoria[-1].lower()
    mib = numero * {"g": 1024, "m": 1}.get(unidade, 1)
    return f"{int(mib * 0.8)}MiB"


def _comando_compose():
    """Comando docker compose da Demo, com as variáveis e os arquivos usados.

    O .env da Demo vem antes do versoes.env, para que as versões fixadas no repositório
    prevaleçam. Sem isso, o .env da tag 3.1.0 deixaria as imagens na versão latest, com
    serviços mais novos que os da tag. Os arquivos compose.agente, compose.override e
    compose.aws, na pasta demo, ajustam a Demo ao experimento.
    """
    memoria = AMBIENTE.get("COLLECTOR_MEM", "1G")
    variaveis = {
        "COLLECTOR_MEM": memoria,
        "COLLECTOR_GOMEMLIMIT": _gomemlimit(memoria),
        "LOCUST_AUTOSTART": "false",   # o Locust só começa quando o orquestrador mandar
        "IP_BACKEND": HOST_BACKEND,
    }
    prefixo = " ".join(f"{k}={shlex.quote(v)}" for k, v in variaveis.items())
    # o agente entra porque o Locust da Demo 3.1.0 faz perguntas a ele (compose.agente.yml)
    arquivos = ("-f compose.yaml -f compose.full.yaml -f ../compose.agente.yml "
                "-f ../compose.override.yml")
    if NA_AWS:
        arquivos += " -f ../compose.aws.yml"
    return f"{prefixo} docker compose --env-file .env --env-file ../../versoes.env {arquivos}"


def _compose_demo(*args):
    """Roda docker compose da Demo na instância 2 com os argumentos dados."""
    return na_instancia2(f"cd demo/opentelemetry-demo && {_comando_compose()} {' '.join(args)}")


def parar_fontes():
    """Para todos os serviços da Demo menos o Collector, que esvazia a fila em seguida.

    Sem isso, a Demo continua mandando telemetria entre a leitura dos contadores e a parada,
    e o backend guarda mais itens do que o Collector contou como enviados.
    """
    dc = _comando_compose()
    # a lista de serviços vem do próprio compose, menos a linha exata otel-collector
    na_instancia2(f"cd demo/opentelemetry-demo && {dc} stop "
                  f"$({dc} config --services | grep -vx otel-collector)")
    time.sleep(15)   # tempo para o Collector esvaziar a fila


def preparar_demo(ferramenta):
    """Clona a Demo se faltar e copia o arquivo de extras do par para o Collector dela.

    A Demo é clonada na tag fixada em versoes.env, só na primeira vez. O Collector da Demo
    carrega otelcol-config-extras.yml por último, então o arquivo do par sobrepõe a
    configuração original só no que define.
    """
    extras, _ = EXTRAS[ferramenta]
    na_instancia2(
        "source versoes.env && "
        "{ [ -d demo/opentelemetry-demo ] || git clone -q --depth 1 --branch \"$DEMO_TAG\" "
        "https://github.com/open-telemetry/opentelemetry-demo.git demo/opentelemetry-demo; } && "
        f"cp demo/extras-{extras}.yml demo/opentelemetry-demo/src/otel-collector/otelcol-config-extras.yml")


def parar_demo():
    """Remove todos os contêineres e volumes da Demo."""
    _compose_demo("down", "-v", "--remove-orphans")


def subir_demo(ferramenta, espera=600):
    """Sobe a Demo inteira com o Locust parado e espera o Locust e o Collector responderem.

    Mesmo com os dois no ar, alguns serviços ainda terminam de iniciar, por isso a espera
    extra de 90 segundos antes de liberar a carga.
    """
    preparar_demo(ferramenta)
    _compose_demo("up", "-d", "--force-recreate")
    limite = time.time() + espera
    while time.time() < limite:
        try:
            if (requests.get(f"{LOCUST}/stats/requests", timeout=5).ok
                    and requests.get(collector.METRICAS, timeout=5).ok):
                time.sleep(90)  # alguns serviços terminam de iniciar depois do Locust
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise TimeoutError(f"a Demo não respondeu em {espera} s")


def iniciar_usuarios(n):
    """Liga n usuários no Locust, todos ativos em cerca de 20 segundos."""
    taxa = max(1, round(n / 20))   # usuários iniciados por segundo
    requests.post(f"{LOCUST}/swarm", data={"user_count": n, "spawn_rate": taxa},
                  timeout=30).raise_for_status()


def parar_usuarios():
    """Para todos os usuários do Locust."""
    requests.get(f"{LOCUST}/stop", timeout=30)


def estatisticas_locust():
    """Requisições, falhas e taxa do Locust desde que os usuários foram ligados.

    A linha Aggregated soma todas as rotas. Os erros guardados são os cinco primeiros
    tipos, o bastante para identificar o problema.
    """
    dados = requests.get(f"{LOCUST}/stats/requests", timeout=30).json()
    total = next((s for s in dados.get("stats", []) if s.get("name") == "Aggregated"), {})
    return {
        "usuarios": dados.get("user_count"),
        "requisicoes": total.get("num_requests"),
        "falhas": total.get("num_failures"),
        "taxa_falhas": round(dados.get("fail_ratio", 0), 4),
        "requisicoes_por_s": round(dados.get("total_rps", 0), 1),
        "erros": [e.get("error") for e in dados.get("errors", [])][:5],
    }


def _nucleos_instancia2():
    """Número de núcleos da instância 2, para converter a CPU em porcentagem da instância."""
    return int(na_instancia2("nproc").strip() or 1)


def _cpu_memoria_instancia2():
    """CPU acumulada e memória somadas dos contêineres da instância 2, e as do Locust."""
    leitura = ler(HOST_DEMO)
    cpu = sum(v.get("cpu_s", 0.0) for v in leitura.values())
    memoria = sum(v.get("memoria_bytes", 0.0) for v in leitura.values())
    locust = leitura.get("load-generator", {})
    return time.time(), cpu, memoria, locust.get("cpu_s", 0.0), locust.get("memoria_bytes", 0.0)


def _volumes():
    """Itens recebidos pelo Collector da Demo em cada sinal, antes dos exportadores."""
    return {sinal: collector.recebidos(sinal) or 0 for sinal in SINAIS.values()}


# ---------------------------------------------------------------- piloto de usuários

def piloto_usuarios(ferramenta, niveis, passo):
    """Aplica cada número de usuários e mostra o custo da Demo, para escolher o do experimento.

    Para cada nível, espera um terço do passo (no máximo 60 segundos) para a carga se
    acomodar e mede o restante. Um nível vale se a instância 2 fica abaixo de LIMITE_CPU_DEMO
    e o Locust não registra falhas. O resultado vai para resultados/piloto-usuarios.
    """
    classe, _ = FERRAMENTAS[ferramenta]
    backend = classe()
    nucleos = None
    print(f"preparando: {ferramenta} na instância 1 e a Demo na instância 2", flush=True)
    collector.parar()
    parar_demo()
    backend.limpar()
    subir_cadvisor(HOST_DEMO)
    backend.subir()
    subir_demo(ferramenta)
    nucleos = _nucleos_instancia2()
    linhas = []
    try:
        for n in niveis:
            print(f"  {n} usuários por {passo} s", flush=True)
            iniciar_usuarios(n)
            time.sleep(min(60, passo // 3))   # acomodação, fora da medida
            t0, cpu0, _, lcpu0, _ = _cpu_memoria_instancia2()
            v0 = _volumes()
            time.sleep(passo - min(60, passo // 3))
            t1, cpu1, memoria, lcpu1, lmemoria = _cpu_memoria_instancia2()
            v1 = _volumes()
            locust = estatisticas_locust()
            parar_usuarios()
            cpu_pct = (cpu1 - cpu0) / (t1 - t0) * 100 / nucleos
            linha = {
                "usuarios": n,
                "cpu_instancia2_pct": round(cpu_pct, 1),
                "memoria_instancia2_gib": round(memoria / 2**30, 1),
                # o gerador de carga roda na mesma instância e não pode ser o limite
                "cpu_locust_pct": round((lcpu1 - lcpu0) / (t1 - t0) * 100),
                "memoria_locust_gib": round(lmemoria / 2**30, 2),
                **{f"{s}_por_s": round((v1[s] - v0[s]) / (t1 - t0)) for s in v0},
                **{f"locust_{k}": v for k, v in locust.items()},
                "dentro_do_limite": cpu_pct < LIMITE_CPU_DEMO and not locust["falhas"],
            }
            linhas.append(linha)
            print(f"    CPU da instância 2 {linha['cpu_instancia2_pct']}%, "
                  f"memória {linha['memoria_instancia2_gib']} GiB, "
                  f"Locust {linha['cpu_locust_pct']}% e {linha['memoria_locust_gib']} GiB, "
                  f"{linha['locust_requisicoes_por_s']} req/s, falhas {locust['falhas']}, "
                  f"logs {linha['log_records_por_s']}/s, spans {linha['spans_por_s']}/s, "
                  f"pontos de métricas {linha['metric_points_por_s']}/s", flush=True)
            if locust["erros"]:
                print(f"    erros do Locust: {locust['erros']}", flush=True)
            time.sleep(30)   # a Demo volta ao repouso antes do próximo nível
    finally:
        parar_usuarios()
        parar_demo()
        backend.limpar()
    pasta = RAIZ / "resultados" / "piloto-usuarios"
    pasta.mkdir(parents=True, exist_ok=True)
    with open(pasta / f"{ferramenta}.json", "w") as arquivo:
        json.dump({"nucleos_instancia2": nucleos, "passo_s": passo, "niveis": linhas},
                  arquivo, indent=2, ensure_ascii=False)
    validos = [l["usuarios"] for l in linhas if l["dentro_do_limite"]]
    print(f"\nmaior número de usuários dentro do limite: {max(validos) if validos else None}, "
          f"resultados em {pasta / (ferramenta + '.json')}")


# ---------------------------------------------------------------- experimento

def executar_realista(ferramenta, usuarios, repeticao, duracao, experimento):
    """Uma execução completa do cenário realista com o número de usuários pedido.

    Além do roteiro comum, sobe a Demo antes da medição, interrompe os serviços que geram
    telemetria antes da leitura dos contadores e remove a Demo antes da descarga. O resumo traz as
    estatísticas do Locust, o volume de telemetria e o disco por item guardado.
    """
    par = PARES[ferramenta]
    _, exportador = EXTRAS[ferramenta]
    execucao = Execucao(experimento, par, ferramenta, f"{usuarios}usuarios", repeticao,
                        aquecimento_s=AQUECIMENTO, medir_disco=True, demo=True)
    if execucao.concluida:
        print(f"[{ferramenta} {usuarios}usuarios rep{repeticao}] já concluída, pulando", flush=True)
        return None
    locust = {}
    volumes = {}

    def antes():
        """Sobe uma Demo nova, já com o backend pronto."""
        print(f"[{ferramenta} {usuarios}usuarios rep{repeticao}] subindo a Demo", flush=True)
        parar_demo()
        subir_demo(ferramenta)

    def carga():
        """Liga os usuários pelo tempo da medição e registra o volume de telemetria."""
        v0 = _volumes()
        iniciar_usuarios(usuarios)
        time.sleep(duracao)
        locust.update(estatisticas_locust())
        parar_usuarios()
        v1 = _volumes()
        volumes.update({s: v1[s] - v0[s] for s in v0})

    def depois():
        """Remove a Demo depois da leitura dos contadores."""
        parar_demo()

    def completar(resumo):
        """Acrescenta ao resumo o Locust, a telemetria, a deduplicação do Loki e o disco por item."""
        resumo["locust"] = locust
        resumo["telemetria_recebida"] = volumes
        if ferramenta == "loki":
            completar_loki(resumo)   # perda real e deduplicação separadas, como na ingestão
        # disco por item: nas métricas, os itens são os pontos enviados, porque armazenados é
        # o número de séries; nos outros pares, os itens guardados no backend
        itens = resumo.get("enviados") if par == "metricas" else resumo.get("armazenados")
        if itens and "disco_liquido_kib" in resumo:
            resumo["bytes_por_item"] = round(resumo["disco_liquido_kib"] * 1024 / itens, 2)

    try:
        resumo = executar(execucao, carga, antes=antes, depois=depois, exportador=exportador,
                          fechar=parar_fontes, completar=completar)
    finally:
        try:
            parar_usuarios()
        except requests.RequestException:
            pass
        parar_demo()

    perda = resumo.get("perda_pct")   # não existe nas métricas, em que se contam séries
    print(f"[{ferramenta} {usuarios}usuarios rep{repeticao}] enviados {resumo.get('enviados')}, "
          f"armazenados {resumo.get('armazenados')}, "
          f"perda {'-' if perda is None else f'{perda}%'}, "
          f"disco {resumo.get('disco_liquido_kib')} KiB, {resumo.get('bytes_por_item')} bytes "
          f"por item, falhas do Locust {locust.get('falhas')}", flush=True)
    return resumo


def main():
    """Lê os argumentos e roda o piloto de usuários ou uma execução do experimento."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="modo", required=True)
    piloto = sub.add_parser("usuarios")
    piloto.add_argument("--ferramenta", default="loki", choices=list(EXTRAS))
    piloto.add_argument("--niveis", type=int, nargs="+", default=[25, 50, 100, 200])
    piloto.add_argument("--passo", type=int, default=300)
    ex = sub.add_parser("execucao")
    ex.add_argument("ferramenta", choices=list(EXTRAS))
    ex.add_argument("--usuarios", type=int, required=True)
    ex.add_argument("--repeticao", type=int, default=1)
    ex.add_argument("--duracao", type=int, default=DURACAO)
    ex.add_argument("--experimento", default="realista")
    args = parser.parse_args()
    if args.modo == "usuarios":
        piloto_usuarios(args.ferramenta, args.niveis, args.passo)
    else:
        if args.duracao <= AQUECIMENTO:
            sys.exit(f"--duracao precisa passar de {AQUECIMENTO} s")
        executar_realista(args.ferramenta, args.usuarios, args.repeticao, args.duracao,
                          args.experimento)


if __name__ == "__main__":
    main()
