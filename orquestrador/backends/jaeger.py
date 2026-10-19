from backends.base import Backend
from backends.elasticsearch import Elasticsearch


class Jaeger(Backend):
    """Usa o Elasticsearch como armazenamento. Disco e contagem são medidos nele."""
    pasta = "jaeger"
    url = "http://localhost:16686"
    saude = "/api/v3/services"
    indice = "*jaeger-span-*"

    def __init__(self):
        self.es = Elasticsearch()

    def subir(self, espera=180):
        self.es.subir(espera)          # o Jaeger encerra se o Elasticsearch não estiver pronto
        super().subir(espera)

    def limpar(self):
        super().limpar()
        self.es.limpar()

    def contar(self):
        return self.es.contar_indice(self.indice)

    def descarregar(self):
        self.es.descarregar()

    def medir_disco(self):
        return self.es.medir_disco()
