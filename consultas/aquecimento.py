"""Fase de aquecimento das consultas, fora dos tempos medidos (A12).

No piloto, a latência caiu ao longo das 30 repetições em quase todas as consultas. Nos logs, a
queda foi de cerca de 1%. No Jaeger e no Zipkin, foi de 17% a 32% e ainda continuava no fim.
Por isso o tamanho do aquecimento não é fixo: as repetições seguem em blocos de 10, e o
aquecimento termina quando a mediana de cada consulta varia no máximo 5% entre dois blocos
seguidos. Se isso não acontecer em 300 repetições, a medição começa assim mesmo e o resumo
marca o aquecimento como incompleto. As repetições do aquecimento ficam em aquecimento.csv.
"""
import csv
import json
import statistics

BLOCO = 10        # repetições por bloco
VARIACAO = 0.05   # variação máxima da mediana entre dois blocos seguidos
MAXIMO = 30       # blocos, ou 300 repetições


class Aquecimento:
    def __init__(self, pasta="/saida"):
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
        self.saida.writerow([len(self.blocos) + 1, inicio, consulta, ms, status, erro])
        self.tempos.setdefault(consulta, []).append(ms)

    def fim_repeticao(self):
        """Chamado ao fim de cada repetição. Devolve True quando o aquecimento termina."""
        self.repeticoes += 1
        if self.repeticoes % BLOCO:
            return False
        medianas = {c: statistics.median(t) for c, t in self.tempos.items()}
        self.tempos = {}
        anterior = self.blocos[-1] if self.blocos else None
        self.blocos.append(medianas)
        estavel = anterior is not None and all(
            abs(m / anterior[c] - 1) <= VARIACAO for c, m in medianas.items() if anterior.get(c))
        if estavel or len(self.blocos) >= MAXIMO:
            self.incompleto = not estavel
            self.encerrar()
        return not self.ativo

    def encerrar(self):
        self.ativo = False
        self.arquivo.close()
        with open(f"{self.pasta}/aquecimento.json", "w") as arquivo:
            json.dump({"repeticoes": self.repeticoes, "incompleto": self.incompleto,
                       "medianas_por_bloco": [{c: round(m, 3) for c, m in b.items()}
                                              for b in self.blocos]},
                      arquivo, indent=2)
