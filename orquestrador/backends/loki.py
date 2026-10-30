"""Loki: backend de logs que indexa só os rótulos e guarda o texto comprimido em blocos."""
import requests

from backends.base import Backend


class Loki(Backend):
    """Loki em modo monolítico, com armazenamento em sistema de arquivos."""
    pasta = "loki"
    volume = "loki-data"
    url = "http://localhost:3100"
    saude = "/ready"

    def contar(self):
        """Número de linhas guardadas na última hora, em todos os serviços.

        A janela de uma hora cobre qualquer execução, que dura bem menos que isso. A consulta
        conta também o que ainda está na memória do ingester. O tempo limite é longo porque,
        depois de uma saturação, a contagem pode demorar minutos.
        """
        consulta = 'sum(count_over_time({service_name=~".+"}[1h]))'
        r = requests.get(f"{self.url}/loki/api/v1/query", params={"query": consulta}, timeout=600)
        r.raise_for_status()
        resultado = r.json()["data"]["result"]
        return int(resultado[0]["value"][1]) if resultado else 0

    def descarregar(self):
        """Pede ao ingester para gravar em disco os blocos que estão em memória."""
        requests.post(f"{self.url}/flush", timeout=600).raise_for_status()
