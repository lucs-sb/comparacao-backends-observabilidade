"""Operações comuns a todos os backends.

Cada backend tem uma pasta em backends/ com o compose.yaml que o sobe na instância 1. A
classe Backend sobe, remove e mede o volume de qualquer um deles. As subclasses definem o
endereço, a verificação de saúde e as operações que mudam de uma ferramenta para outra: como
contar os itens guardados e como forçar a gravação dos dados em disco.
"""
import subprocess
import time

import requests

from config import ARQUIVO_VERSOES, HOST_BACKEND, RAIZ, arquivos_compose, docker_em


class Backend:
    """Base dos backends. As subclasses preenchem os atributos e implementam contar e descarregar."""
    pasta: str          # pasta em backends/ com o compose.yaml
    volume: str = None  # volume Docker com os dados
    url: str            # endereço acessado a partir da máquina que roda o orquestrador
    saude: str          # caminho que responde 200 quando o backend está pronto

    def __init_subclass__(cls, **kwargs):
        """Ajusta o endereço de cada subclasse quando ela é definida."""
        super().__init_subclass__(**kwargs)
        # As classes escrevem o endereço com localhost, trocado aqui pelo IP da instância 1 na AWS
        if "url" in cls.__dict__:
            cls.url = cls.url.replace("localhost", HOST_BACKEND)

    def _variaveis(self):
        """Variáveis extras para o compose. As classes que precisam de valores próprios,
        como o Elasticsearch com memória diferente em cada par, sobrescrevem este método."""
        return {}

    def _compose(self, *args):
        """Roda docker compose na pasta do backend, na instância 1."""
        pasta = RAIZ / "backends" / self.pasta
        subprocess.run(
            ["docker", "compose", "--env-file", str(ARQUIVO_VERSOES),
             *arquivos_compose(pasta), *args],
            cwd=pasta,
            env={**docker_em(HOST_BACKEND), **self._variaveis()},
            check=True,
            capture_output=True,
        )

    def subir(self, espera=180):
        """Sobe o backend e espera o caminho de saúde responder, por até espera segundos."""
        self._compose("up", "-d")
        limite = time.time() + espera
        while time.time() < limite:
            try:
                if requests.get(self.url + self.saude, timeout=2).ok:
                    return
            except requests.RequestException:
                pass
            time.sleep(2)
        raise TimeoutError(f"{self.pasta} não respondeu em {espera} s")

    def limpar(self):
        """Remove o contêiner e o volume de dados, deixando o backend como novo."""
        self._compose("down", "-v")

    def medir_disco(self):
        """Tamanho do volume em KiB, medido com du num contêiner que monta o volume."""
        saida = subprocess.run(
            ["docker", "run", "--rm", "-v", f"{self.volume}:/d", "alpine", "du", "-sk", "/d"],
            env=docker_em(HOST_BACKEND),
            capture_output=True, text=True, check=True,
        ).stdout
        return int(saida.split()[0])

    def medir_pastas(self):
        """Tamanho em KiB de cada pasta e arquivo da raiz do volume, como WAL e dados.

        O padrão .[!.]* inclui as pastas ocultas, sem pegar . e .., e a saída de erro é
        descartada porque o padrão falha quando não há pastas ocultas.
        """
        saida = subprocess.run(
            ["docker", "run", "--rm", "-v", f"{self.volume}:/d", "alpine", "sh", "-c",
             "cd /d && du -sk * .[!.]* 2>/dev/null"],
            env=docker_em(HOST_BACKEND),
            capture_output=True, text=True,
        ).stdout
        pastas = {}
        for linha in saida.splitlines():
            tamanho, _, nome = linha.partition("\t")
            if tamanho.isdigit():
                pastas[nome] = int(tamanho)
        return pastas

    def contar(self):
        """Número de itens guardados: logs, spans ou, nas métricas, séries."""
        raise NotImplementedError

    def descarregar(self):
        """Força o backend a gravar em disco o que ainda está em memória."""
        raise NotImplementedError

    def acomodar(self):
        """Espera o backend terminar a reorganização dos dados antes da medida de disco.

        Por padrão não há o que esperar. O Elasticsearch espera as fusões de segmentos.
        """
