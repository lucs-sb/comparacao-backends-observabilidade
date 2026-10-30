"""Operações comuns a todos os backends."""
import subprocess
import time

import requests

from config import ARQUIVO_VERSOES, HOST_BACKEND, RAIZ, arquivos_compose, docker_em


class Backend:
    pasta: str          # pasta em backends/ com o compose.yaml
    volume: str = None  # volume Docker com os dados
    url: str            # endereço acessado a partir da máquina que roda o orquestrador
    saude: str          # caminho que responde 200 quando o backend está pronto

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # As classes escrevem o endereço com localhost, trocado aqui pelo IP da instância 1 na AWS
        if "url" in cls.__dict__:
            cls.url = cls.url.replace("localhost", HOST_BACKEND)

    def _variaveis(self):
        """Variáveis extras para o compose. As classes que precisam de valores próprios,
        como o Elasticsearch com memória diferente em cada par, sobrescrevem este método."""
        return {}

    def _compose(self, *args):
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
        self._compose("down", "-v")

    def medir_disco(self):
        """Tamanho do volume em KiB."""
        saida = subprocess.run(
            ["docker", "run", "--rm", "-v", f"{self.volume}:/d", "alpine", "du", "-sk", "/d"],
            env=docker_em(HOST_BACKEND),
            capture_output=True, text=True, check=True,
        ).stdout
        return int(saida.split()[0])

    def medir_pastas(self):
        """Tamanho em KiB de cada pasta e arquivo da raiz do volume, como WAL e dados (A31)."""
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
        raise NotImplementedError

    def descarregar(self):
        raise NotImplementedError

    def acomodar(self):
        """Espera o backend terminar a reorganização dos dados antes da medida de disco.

        Por padrão não há o que esperar. O Elasticsearch espera as fusões de segmentos (A27).
        """
