"""Configuração comum a todo o orquestrador.

Reúne os caminhos do repositório, as versões fixadas em versoes.env, os endereços das
instâncias lidos de ambiente.env e as funções que fazem um comando docker ou de terminal
rodar na instância certa.

O mesmo código roda em dois ambientes. Sem o arquivo ambiente.env, tudo fica na máquina
local, como no MacBook usado no desenvolvimento. Com ele, o orquestrador roda na instância 3
da AWS e comanda o Docker das instâncias 1 (backends) e 2 (Collector e Demo) por SSH.
"""
import os
import subprocess
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]   # raiz do repositório, um nível acima desta pasta
ARQUIVO_VERSOES = RAIZ / "versoes.env"
ARQUIVO_AMBIENTE = RAIZ / "ambiente.env"

# Cada ferramenta avaliada e o par a que ela pertence. As duas ferramentas de um par são
# comparadas entre si.
PARES = {"loki": "logs", "elasticsearch": "logs", "jaeger": "traces", "zipkin": "traces",
         "prometheus": "metricas", "victoriametrics": "metricas"}
NOMES_BACKENDS = list(PARES)


def _ler(arquivo):
    """Lê um arquivo no formato CHAVE=valor e devolve um dicionário.

    Ignora linhas vazias, linhas de comentário e comentários no fim da linha. Um arquivo
    ausente resulta num dicionário vazio, o que permite rodar sem ambiente.env.
    """
    valores = {}
    if not arquivo.exists():
        return valores
    for linha in arquivo.read_text().splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, valor = linha.split("=", 1)
        valores[chave.strip()] = valor.split("#", 1)[0].strip()
    return valores


VERSOES = _ler(ARQUIVO_VERSOES)

# A presença de IP_BACKEND no ambiente.env indica a execução na AWS.
AMBIENTE = _ler(ARQUIVO_AMBIENTE)
NA_AWS = "IP_BACKEND" in AMBIENTE
HOST_BACKEND = AMBIENTE.get("IP_BACKEND", "localhost")   # instância 1
HOST_DEMO = AMBIENTE.get("IP_DEMO", "localhost")         # instância 2
HOST_CARGA = AMBIENTE.get("IP_CARGA", "localhost")       # instância 3, onde roda o orquestrador


def docker_em(host):
    """Variáveis de ambiente para que os comandos docker atuem na instância indicada.

    Na AWS, DOCKER_HOST aponta para o Docker da instância remota por SSH. Na instância 3, que
    é a própria máquina do orquestrador, o Docker local é usado. As variáveis do ambiente.env
    também são repassadas, porque os arquivos compose leem delas os limites de memória.
    """
    env = {**os.environ, **AMBIENTE}
    if NA_AWS and host != HOST_CARGA:
        env["DOCKER_HOST"] = f"ssh://ubuntu@{host}"
    return env


def arquivos_compose(pasta):
    """Argumentos -f do docker compose para uma pasta.

    Na AWS, acrescenta o compose.aws.yml da pasta, quando existir. Esses arquivos só contêm
    o que muda entre as máquinas, como os nomes dos backends apontando para o IP privado da
    instância 1.
    """
    arquivos = ["-f", "compose.yaml"]
    if NA_AWS and (pasta / "compose.aws.yml").exists():
        arquivos += ["-f", "compose.aws.yml"]
    return arquivos


def hosts_extras():
    """Argumentos --add-host para os contêineres criados com docker run.

    Os geradores e o Locust acessam os backends e o Collector pelo nome, como loki ou
    otelcol. Localmente, a rede Docker tcc resolve esses nomes. Na AWS, cada serviço está em
    outra instância, então cada nome passa a apontar para o IP privado correspondente.
    """
    if not NA_AWS:
        return []
    argumentos = []
    for nome in NOMES_BACKENDS:
        argumentos += ["--add-host", f"{nome}:{HOST_BACKEND}"]
    return argumentos + ["--add-host", f"otelcol:{HOST_DEMO}"]


def na_instancia2(comando, verificar=True):
    """Roda um comando de terminal na raiz do repositório da instância 2 e devolve a saída.

    Usado para a Demo e para os arquivos de logs do experimento de consulta, que precisam
    estar no disco da instância 2. Localmente, roda na própria máquina. Com verificar, um
    código de saída diferente de zero vira uma exceção com o fim da saída de erro.
    """
    pasta = "~/comparacao-backends-observabilidade" if NA_AWS else str(RAIZ)
    completo = f"cd {pasta} && {comando}"
    if NA_AWS:
        argumentos = ["ssh", f"ubuntu@{HOST_DEMO}", completo]
    else:
        argumentos = ["bash", "-c", completo]
    resultado = subprocess.run(argumentos, capture_output=True, text=True)
    if verificar and resultado.returncode != 0:
        raise RuntimeError(f"falhou na instância 2: {comando}\n{resultado.stderr[-2000:]}")
    return resultado.stdout
