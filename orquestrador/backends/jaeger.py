"""Jaeger: backend de traces que guarda os spans no Elasticsearch."""
from backends.base import Backend
from backends.elasticsearch import Elasticsearch


class Jaeger(Backend):
    """Usa o Elasticsearch como armazenamento. Disco e contagem são medidos nele.

    Subir e limpar tratam os dois contêineres. As operações sobre os dados passam direto ao
    Elasticsearch, que é onde os spans ficam.
    """
    pasta = "jaeger"
    url = "http://localhost:16686"
    saude = "/api/v3/services"
    indice = "*jaeger-span-*"

    def __init__(self):
        """Cria o Elasticsearch de armazenamento, com a memória do par de traces."""
        self.es = Elasticsearch()
        self.es.perfil = "traces"

    def subir(self, espera=180):
        """Sobe o Elasticsearch antes, porque o Jaeger encerra se ele não estiver pronto."""
        self.es.subir(espera)
        super().subir(espera)

    def limpar(self):
        """Remove o Jaeger e depois o Elasticsearch, com os dados."""
        super().limpar()
        self.es.limpar()

    def contar(self):
        """Número de spans nos índices de spans."""
        return self.es.contar_indice(self.indice)

    def descarregar(self):
        """Grava os dados pendentes no Elasticsearch."""
        self.es.descarregar()

    def acomodar(self):
        """Espera as fusões de segmentos do Elasticsearch."""
        self.es.acomodar()

    def medir_disco(self):
        """Volume do Elasticsearch, onde ficam os spans."""
        return self.es.medir_disco()

    def medir_pastas(self):
        """Pastas do volume do Elasticsearch."""
        return self.es.medir_pastas()
