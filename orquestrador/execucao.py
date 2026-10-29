"""Roteiro de uma execução, nos seis passos da seção 4.7.

Uso, a partir da pasta orquestrador, para testar com uma ingestão curta no Loki:
    python execucao.py teste
"""
import json
import subprocess
import sys
import time
from contextlib import ExitStack
from dataclasses import asdict, dataclass

import collector
import geradores
from backends.elasticsearch import Elasticsearch
from backends.jaeger import Jaeger
from backends.loki import Loki
from backends.prometheus import Prometheus
from backends.victoriametrics import VictoriaMetrics
from backends.zipkin import Zipkin
from config import HOST_BACKEND, HOST_DEMO, NA_AWS, VERSOES
from medicao import RESULTADOS, Medicao, resumir, subir_cadvisor
from verificar import ESPERA_LOTE, TODOS, medir_disco_estavel

# ferramenta: (classe, arquivo de par do Collector ou None quando a carga vai direto ao backend)
FERRAMENTAS = {
    "loki": (Loki, "logs-loki"),
    "elasticsearch": (Elasticsearch, "logs-es"),
    "jaeger": (Jaeger, "traces-jaeger"),
    "zipkin": (Zipkin, "traces-zipkin"),
    "prometheus": (Prometheus, None),
    "victoriametrics": (VictoriaMetrics, None),
}
SINAIS = {"logs": "log_records", "traces": "spans", "metricas": "metric_points"}


@dataclass
class Execucao:
    experimento: str        # saturacao, ingestao, realista, consulta ou cardinalidade
    par: str                # logs, traces ou metricas
    ferramenta: str         # chave de FERRAMENTAS
    condicao: str           # nível da carga, por exemplo 50pct ou 100mil
    repeticao: int
    aquecimento_s: int = 120  # descartado no resumo de CPU e memória
    # O disco só é medido no cenário realista. Nos sintéticos, o conteúdo repetido dos
    # geradores não representa a compressão de dados reais (A27).
    medir_disco: bool = False
    # No cenário realista, a carga vem da Demo e passa pelo Collector dela, na instância 2,
    # em vez do Collector isolado. O consumo da instância 2 inclui então a Demo inteira.
    demo: bool = False

    @property
    def pasta(self):
        return (RESULTADOS / self.experimento / self.par / self.ferramenta
                / self.condicao / f"rep{self.repeticao}")

    @property
    def concluida(self):
        # o resumo é gravado por último, então só existe se a execução terminou
        return (self.pasta / "resumo.json").exists()


def esvaziar_cache():
    """Esvazia o cache de disco das instâncias 1 e 2. No MacBook, não faz nada."""
    if not NA_AWS:
        return
    for host in (HOST_BACKEND, HOST_DEMO):
        subprocess.run(
            ["ssh", f"ubuntu@{host}", "sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null"],
            check=True, capture_output=True,
        )


def executar(execucao, carga, antes=None, depois=None, exportador=None):
    """Executa o roteiro completo. carga é uma função sem argumentos que aplica a carga.

    antes roda depois do backend pronto e antes da medição, como a subida da Demo. depois
    roda depois da leitura dos contadores do Collector, como a parada da Demo. exportador
    limita a contagem de enviados a um exportador, quando o Collector tem mais de um.
    """
    classe, par_collector = FERRAMENTAS[execucao.ferramenta]
    backend = classe()
    usa_collector = par_collector is not None and not execucao.demo  # Collector isolado
    le_collector = usa_collector or execucao.demo   # contadores na porta 8888 da instância 2
    arquivo_instancia2 = "recursos_demo.csv" if execucao.demo else "recursos_collector.csv"
    execucao.pasta.mkdir(parents=True, exist_ok=True)
    resumo = {"execucao": asdict(execucao), "versoes": VERSOES}

    def etapa(texto):
        print(f"[{execucao.ferramenta} {execucao.condicao} rep{execucao.repeticao}] {texto}", flush=True)

    # 1. Limpeza do ambiente
    etapa("limpando o ambiente")
    collector.parar()
    for classe_backend in TODOS:
        classe_backend().limpar()
    esvaziar_cache()

    # 2. Inicialização, com o volume vazio medido para ser descontado depois (A2)
    etapa("subindo cAdvisor e backend")
    subir_cadvisor(HOST_BACKEND)
    if le_collector:
        subir_cadvisor(HOST_DEMO)
    backend.subir()
    if execucao.medir_disco:
        leituras, _, _ = medir_disco_estavel(backend)
        resumo["disco_vazio_kib"] = leituras[-1]
    if usa_collector:
        etapa(f"subindo o Collector com {par_collector}.yaml")
        collector.subir(par_collector)
    if antes:
        antes()

    # 3. Aplicação da carga, com CPU e memória lidas a cada segundo nas duas instâncias
    etapa("aplicando a carga")
    with ExitStack() as medicoes:
        medicoes.enter_context(Medicao(execucao.pasta / "recursos_backend.csv", HOST_BACKEND))
        if le_collector:
            medicoes.enter_context(Medicao(execucao.pasta / arquivo_instancia2, HOST_DEMO))
        resumo["inicio_carga"] = time.time()
        carga()
        resumo["fim_carga"] = time.time()
        time.sleep(ESPERA_LOTE)  # o Collector esvazia a fila ainda dentro da medição

    # 4. Registro do que foi enviado, antes de parar o Collector
    if le_collector:
        sinal = SINAIS[execucao.par]
        resumo["enviados"] = collector.enviados(sinal, exportador)
        resumo["falhas_collector"] = collector.falhas(sinal, exportador) or 0
    if usa_collector:
        collector.parar()
    if depois:
        depois()

    # 5. Descarga e medição do disco
    etapa("descarregando, contando e medindo o disco" if execucao.medir_disco
          else "descarregando e contando")
    backend.descarregar()
    resumo["armazenados"] = backend.contar()
    # Nas métricas, armazenados é o número de séries, e não de amostras, então não há perda
    # a calcular pela contagem. A verificação fica com as falhas de envio do Collector.
    if resumo.get("enviados") and execucao.par != "metricas":
        perda = resumo["enviados"] - resumo["armazenados"]
        resumo["perda_pct"] = round(100 * perda / resumo["enviados"], 3)
    if execucao.medir_disco:
        inicio_acomodacao = time.time()
        backend.acomodar()
        resumo["espera_acomodacao_s"] = round(time.time() - inicio_acomodacao)
        leituras, estavel, espera = medir_disco_estavel(backend)
        resumo["disco_final_kib"] = leituras[-1]
        resumo["disco_liquido_kib"] = leituras[-1] - resumo["disco_vazio_kib"]
        resumo["disco_estavel"] = estavel
        resumo["espera_disco_s"] = round(espera)

    # 6. Exportação: os CSV brutos já estão na pasta, e o resumo marca a execução como concluída
    resumo["recursos_backend"] = resumir(execucao.pasta / "recursos_backend.csv",
                                         execucao.aquecimento_s)
    if le_collector:
        chave = "recursos_demo" if execucao.demo else "recursos_collector"
        resumo[chave] = resumir(execucao.pasta / arquivo_instancia2, execucao.aquecimento_s)
    with open(execucao.pasta / "resumo.json", "w") as arquivo:
        json.dump(resumo, arquivo, indent=2, ensure_ascii=False)
    etapa(f"concluída, resultados em {execucao.pasta}")
    return resumo


def teste():
    """Ingestão de 1000 logs por segundo no Loki durante 60 segundos."""
    execucao = Execucao("teste", "logs", "loki", "1000ps", 1, aquecimento_s=10)

    def carga():
        geradores.telemetrygen("logs", "--rate", "1000", "--duration", "60s",
                               "--body", "teste tcc", "--service", "tcc-teste")

    resumo = executar(execucao, carga)
    print(json.dumps({k: v for k, v in resumo.items() if k != "versoes"},
                     indent=2, ensure_ascii=False))


if __name__ == "__main__":
    if sys.argv[1:] == ["teste"]:
        teste()
    else:
        sys.exit(__doc__)
