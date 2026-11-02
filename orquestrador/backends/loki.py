"""Loki: backend de logs que indexa só os rótulos e guarda o texto comprimido em blocos."""
import time

import requests

from backends.base import Backend

# blocos na memória do ingester e blocos esperando gravação
MEMORIA = ("loki_ingester_memory_chunks", "loki_ingester_flush_queue_length")
# checkpoints iniciados, checkpoints com séries concluídos e streams em memória
CHECKPOINT = ("loki_ingester_checkpoint_creations_total",
              "loki_ingester_checkpoint_duration_seconds_count", "loki_ingester_memory_streams")


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

    def ingester(self, nomes=MEMORIA):
        """Valores das métricas do ingester pedidas, lidos em /metrics e somados entre rótulos."""
        valores = dict.fromkeys(nomes, 0.0)
        r = requests.get(f"{self.url}/metrics", timeout=30)
        r.raise_for_status()
        for linha in r.text.splitlines():
            nome = linha.split("{", 1)[0].split(" ", 1)[0]
            if nome in valores:
                valores[nome] += float(linha.rsplit(" ", 1)[1])
        return valores

    def esperar_memoria(self, limite_s=300):
        """Espera o ingester liberar da memória os blocos já gravados em disco.

        Depois da descarga, os blocos continuam na memória do ingester até a varredura
        seguinte, que acontece a cada 30 s, e até lá cada consulta lê a cópia em memória e a
        do disco. No diagnóstico, a busca textual levou cerca de 335 ms nesse intervalo e
        155 ms depois dele. A espera termina com a memória e a fila de descarga vazias, ou
        depois de limite_s segundos. Devolve se a memória foi liberada.
        """
        limite = time.time() + limite_s
        while any(self.ingester().values()):
            if time.time() > limite:
                return False
            time.sleep(2)
        return True

    def esperar_checkpoint(self, limite_s=600):
        """Espera um checkpoint do WAL feito depois da descarga, antes da medida de disco.

        O WAL é uma cópia sem compressão do que chegou, usada só para recuperar a memória do
        ingester se o Loki cair. A cada 5 minutos, um checkpoint grava o que ainda não foi
        descarregado e apaga os segmentos antigos. No cenário realista, o WAL ocupou de 7 MB,
        com a medida depois de um checkpoint, a 83 MB, com a medida no meio de um, para os
        mesmos dados. A espera deixa o WAL no mesmo estado em todas as execuções: com a memória
        já liberada, o checkpoint seguinte não tem séries a gravar e apaga os segmentos com os
        dados já guardados nos chunks. É o equivalente ao log de transações do Elasticsearch,
        esvaziado na descarga. Devolve se o checkpoint terminou dentro de limite_s segundos.
        """
        limite = time.time() + limite_s
        if not self.esperar_memoria(limite_s):
            return False
        criados, concluidos, _ = self.ingester(CHECKPOINT).values()
        while True:
            agora_criados, agora_concluidos, streams = self.ingester(CHECKPOINT).values()
            if agora_criados > criados:
                if not streams:
                    # sem séries, o checkpoint termina logo depois de começar e não entra na
                    # contagem dos concluídos, então basta dar tempo à troca dos arquivos
                    time.sleep(5)
                    return True
                if agora_concluidos > concluidos:
                    return True
            if time.time() > limite:
                return False
            time.sleep(5)
