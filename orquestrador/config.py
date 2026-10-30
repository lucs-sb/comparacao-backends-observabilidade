"""Caminhos do repositório, versões de versoes.env e endereços de ambiente.env."""
import os
import subprocess
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
ARQUIVO_VERSOES = RAIZ / "versoes.env"
ARQUIVO_AMBIENTE = RAIZ / "ambiente.env"
# ferramenta: par que ela avalia
PARES = {"loki": "logs", "elasticsearch": "logs", "jaeger": "traces", "zipkin": "traces",
         "prometheus": "metricas", "victoriametrics": "metricas"}
NOMES_BACKENDS = list(PARES)


def _ler(arquivo):
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

# Sem ambiente.env, tudo roda no MacBook. Com ele, o orquestrador roda na instância 3
# e comanda o Docker das instâncias 1 e 2 por SSH.
AMBIENTE = _ler(ARQUIVO_AMBIENTE)
NA_AWS = "IP_BACKEND" in AMBIENTE
HOST_BACKEND = AMBIENTE.get("IP_BACKEND", "localhost")
HOST_DEMO = AMBIENTE.get("IP_DEMO", "localhost")
HOST_CARGA = AMBIENTE.get("IP_CARGA", "localhost")  # a própria máquina do orquestrador


def docker_em(host):
    """Variáveis de ambiente para que os comandos docker atuem na instância indicada."""
    env = {**os.environ, **AMBIENTE}
    if NA_AWS and host != HOST_CARGA:  # na instância 3, o Docker é o local
        env["DOCKER_HOST"] = f"ssh://ubuntu@{host}"
    return env


def arquivos_compose(pasta):
    """Na AWS, acrescenta o compose.aws.yml da pasta, quando existir."""
    arquivos = ["-f", "compose.yaml"]
    if NA_AWS and (pasta / "compose.aws.yml").exists():
        arquivos += ["-f", "compose.aws.yml"]
    return arquivos


def hosts_extras():
    """Na AWS, faz os nomes dos backends e do Collector apontarem para os IPs privados."""
    if not NA_AWS:
        return []
    argumentos = []
    for nome in NOMES_BACKENDS:
        argumentos += ["--add-host", f"{nome}:{HOST_BACKEND}"]
    return argumentos + ["--add-host", f"otelcol:{HOST_DEMO}"]


def na_instancia2(comando, verificar=True):
    """Roda um comando na raiz do repositório da instância 2 (ou local, no MacBook)."""
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
