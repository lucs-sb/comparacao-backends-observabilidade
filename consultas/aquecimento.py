"""Fase de aquecimento das consultas, fora dos tempos medidos.

As primeiras consultas a um backend são mais lentas que as seguintes, porque os dados ainda
não estão na memória, os caches estão vazios e o Java ainda não otimizou o código. Medir
essas consultas misturaria o custo de começar do zero com o desempenho normal da ferramenta.

No piloto, a latência caiu ao longo das 30 repetições em quase todas as consultas. Nos logs, a
queda foi de cerca de 1%. No Jaeger e no Zipkin, foi de 17% a 32% e ainda continuava no fim.
Por isso o tamanho do aquecimento não é fixo: as repetições seguem em blocos de 10, e o
aquecimento termina quando as medianas de cada consulta nos três últimos blocos ficam dentro
de 5% umas das outras. Três blocos são 30 repetições, o mesmo tamanho da medição. Comparar só
dois blocos seguidos não basta: no Jaeger, a latência caiu de 3% a 5% por bloco durante quase
100 repetições, uma queda que passava bloco a bloco e se acumulava na medição. Se a condição
não for atingida em 500 repetições, a medição começa assim mesmo e o resumo marca o
aquecimento como incompleto. As repetições do aquecimento ficam em aquecimento.csv.
"""
import csv
import json
import statistics

BLOCO = 10        # repetições por bloco
JANELA = 3        # blocos comparados, ou 30 repetições, o tamanho da medição
VARIACAO = 0.05   # diferença máxima entre a maior e a menor mediana da janela
MAXIMO = 50       # blocos, ou 500 repetições


class Aquecimento:
    """Acompanha as repetições do aquecimento e decide quando ele termina.

    Enquanto ativo for verdadeiro, os tempos vão para aquecimento.csv e não para os tempos
    medidos. Ao terminar, grava aquecimento.json com o número de repetições, se ficou
    incompleto e as medianas de cada bloco.
    """

    def __init__(self, pasta="/saida"):
        """pasta é onde os arquivos são gravados, montada pelo orquestrador no contêiner."""
        self.pasta = pasta
        self.ativo = True
        self.incompleto = False
        self.repeticoes = 0
        self.blocos = []          # medianas de cada consulta em cada bloco
        self.tempos = {}          # tempos do bloco atual, por consulta
        self.arquivo = open(f"{pasta}/aquecimento.csv", "w", newline="")
        self.saida = csv.writer(self.arquivo)
        self.saida.writerow(["bloco", "inicio", "consulta", "ms", "status", "erro"])

    def registrar(self, inicio, consulta, ms, status, erro):
        """Guarda o tempo de uma consulta do aquecimento."""
        self.saida.writerow([len(self.blocos) + 1, inicio, consulta, ms, status, erro])
        self.tempos.setdefault(consulta, []).append(ms)

    def fim_repeticao(self):
        """Chamado ao fim de cada repetição. Devolve True quando o aquecimento termina."""
        self.repeticoes += 1
        if self.repeticoes % BLOCO:
            return False   # o bloco ainda não fechou
        medianas = {c: statistics.median(t) for c, t in self.tempos.items()}
        self.tempos = {}
        self.blocos.append(medianas)
        # estável quando, em todas as consultas, a maior mediana dos últimos JANELA blocos
        # passa da menor em no máximo VARIACAO
        janela = self.blocos[-JANELA:]
        estavel = len(janela) == JANELA and all(
            max(b[c] for b in janela) <= (1 + VARIACAO) * min(b[c] for b in janela)
            for c in medianas)
        if estavel or len(self.blocos) >= MAXIMO:
            self.incompleto = not estavel
            self.encerrar()
        return not self.ativo

    def encerrar(self):
        """Encerra o aquecimento e grava o aquecimento.json."""
        self.ativo = False
        self.arquivo.close()
        with open(f"{self.pasta}/aquecimento.json", "w") as arquivo:
            json.dump({"repeticoes": self.repeticoes, "incompleto": self.incompleto,
                       "medianas_por_bloco": [{c: round(m, 3) for c, m in b.items()}
                                              for b in self.blocos]},
                      arquivo, indent=2)
