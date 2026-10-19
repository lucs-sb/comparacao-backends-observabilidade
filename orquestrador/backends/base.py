"""Operações comuns a todos os backends."""
import subprocess
import time

import requests

from config import ARQUIVO_VERSOES, RAIZ


class Backend:
    pasta: str          # pasta em backends/ com o compose.yaml
    volume: str = None  # volume Docker com os dados
    url: str            # endereço acessado a partir da máquina que roda o orquestrador
    saude: str          # caminho que responde 200 quando o backend está pronto

    def _compose(self, *args):
        subprocess.run(
            ["docker", "compose", "--env-file", str(ARQUIVO_VERSOES), *args],
            cwd=RAIZ / "backends" / self.pasta,
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
            capture_output=True, text=True, check=True,
        ).stdout
        return int(saida.split()[0])

    def contar(self):
        raise NotImplementedError

    def descarregar(self):
        raise NotImplementedError
