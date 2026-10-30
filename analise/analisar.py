"""Análise dos resultados: tabelas com teste estatístico e figuras para a monografia.

Uso, a partir da raiz do repositório:
    python3 analise/analisar.py [resultados] [--saida analise]

Lê o resumo.json de cada execução dos cinco experimentos e gera:

- analise/tabelas/*.tex: o ambiente tabular de cada tabela, no formato do pacote booktabs,
  pronto para entrar no ambiente table da monografia;
- analise/tabelas/*.csv: os mesmos números, sem formatação, para conferência;
- analise/figuras/*.pdf: as figuras, vetoriais, na largura da área de texto;
- analise/tabelas/verificacao.csv: as execuções que merecem atenção antes da análise.

A unidade de análise é a execução. Em cada par, a primeira ferramenta é a referência: Loki,
Prometheus e Jaeger. As duas ferramentas são comparadas pelo teste de Mann-Whitney bilateral,
com nível de significância de 5% e sem correção para comparações múltiplas. O tamanho do
efeito é a razão entre as medianas, a da ferramenta comparada dividida pela da referência.
Uma razão de 2 significa que a comparada teve o dobro do valor da referência. O intervalo de
confiança de 95% da razão vem de reamostragem (bootstrap), sem supor uma distribuição para
os dados. A figura resumo.pdf reúne as principais razões de todos os experimentos.

Requer as bibliotecas de requirements-analise.txt. Para testar sem os resultados reais, gere
dados simulados com analise/simular.py.
"""
import argparse
import csv
import json
import statistics
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")   # gera arquivos sem precisar de tela
import matplotlib.pyplot as plt   # noqa: E402
import numpy as np   # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator   # noqa: E402
from scipy.stats import mannwhitneyu   # noqa: E402

# ---------------------------------------------------------------- definições

# ordem dos pares nas figuras e, em cada par, a referência primeiro
PARES = {"logs": ("loki", "elasticsearch"),
         "metricas": ("prometheus", "victoriametrics"),
         "traces": ("jaeger", "zipkin")}
NOME_PAR = {"logs": "Logs", "metricas": "Métricas", "traces": "Traces"}
NOME = {"loki": "Loki", "elasticsearch": "Elasticsearch", "prometheus": "Prometheus",
        "victoriametrics": "VictoriaMetrics", "jaeger": "Jaeger", "zipkin": "Zipkin"}
ITEM = {"logs": "logs/s", "metricas": "amostras/s", "traces": "spans/s"}
# a referência de cada par em azul e a comparada em laranja, em todas as figuras
COR = {"referencia": "#2a78d6", "comparada": "#eb6834"}
CINZA = "#52514e"
NIVEIS = (25, 50, 75)
PATAMARES = (("100mil", 100_000), ("500mil", 500_000), ("1milhao", 1_000_000))
NOME_PATAMAR = {100_000: "100 mil", 500_000: "500 mil", 1_000_000: "1 milhão"}
CONSULTAS = {"logs": (("filtro_rotulo", "Filtro por rótulo"),
                      ("busca_textual", "Busca textual")),
             "traces": (("busca_servico", "Serviço"),
                        ("busca_atributo", "Atributo"),
                        ("busca_identificador", "Identificador"))}
ALFA = 0.05
REAMOSTRAGENS = 10_000   # reamostragens do bootstrap para o intervalo da razão
RESUMO = []              # comparações escolhidas para a figura-resumo
LARGURA = 6.3   # polegadas, cerca de 16 cm, a largura da área de texto


def cor(ferramenta):
    """Cor da ferramenta: azul para a referência do par e laranja para a comparada."""
    for referencia, comparada in PARES.values():
        if ferramenta == referencia:
            return COR["referencia"]
        if ferramenta == comparada:
            return COR["comparada"]
    raise KeyError(ferramenta)


# ---------------------------------------------------------------- formatação brasileira

def numero(valor, casas=1):
    """Número com vírgula decimal e ponto nos milhares, como 1.234,5."""
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "--"
    texto = f"{valor:,.{casas}f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")


def valor_p(p):
    """p-valor com três casas, ou < 0,001, no formato do LaTeX."""
    return "$<$ 0,001" if p < 0.001 else numero(p, 3)


def latex(texto):
    """Escapa os caracteres que o LaTeX interpretaria, como o sinal de porcentagem."""
    return texto.replace("%", "\\%")


def _marca(x, _):
    """Número de uma marca de eixo, com vírgula decimal e ponto nos milhares."""
    if float(x).is_integer():
        return numero(x, 0)
    return f"{x:.4g}".replace(".", ",")


def eixo_brasileiro(eixo, x=True):
    """Troca o ponto decimal por vírgula nos números dos eixos.

    Com x falso, só o eixo vertical muda, porque o horizontal tem nomes, e não números.
    """
    if x:
        eixo.xaxis.set_major_formatter(FuncFormatter(_marca))
    eixo.yaxis.set_major_formatter(FuncFormatter(_marca))


def estilo():
    """Estilo das figuras: fonte com serifa, traços finos e grade discreta."""
    plt.rcParams.update({
        "font.family": "serif",
        "font.serif": ["Times New Roman", "Times", "Liberation Serif", "DejaVu Serif"],
        "mathtext.fontset": "stix",
        "font.size": 9, "axes.titlesize": 9, "axes.labelsize": 9,
        "xtick.labelsize": 8, "ytick.labelsize": 8, "legend.fontsize": 8,
        "axes.linewidth": 0.6, "axes.edgecolor": CINZA,
        "xtick.color": CINZA, "ytick.color": CINZA,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "axes.grid.axis": "y",
        "grid.color": "#e3e2de", "grid.linewidth": 0.5,
        "legend.frameon": False, "lines.linewidth": 1.4,
        "pdf.fonttype": 42,   # fontes embutidas como texto, e não como contornos
        "savefig.bbox": "tight", "savefig.pad_inches": 0.03,
    })


# ---------------------------------------------------------------- leitura

def carregar(raiz):
    """Todos os resumos, agrupados por experimento.

    Cada item traz a pasta da execução e o conteúdo do resumo.json. As pastas que começam
    com _ (falhas e registros do rodar.py) ficam de fora.
    """
    execucoes = {}
    for arquivo in sorted(Path(raiz).glob("*/*/*/*/rep*/resumo.json")):
        if any(parte.startswith("_") for parte in arquivo.parts):
            continue
        resumo = json.loads(arquivo.read_text())
        e = resumo["execucao"]
        execucoes.setdefault(e["experimento"], []).append({"pasta": arquivo.parent, **resumo})
    return execucoes


def filtrar(execucoes, ferramenta, condicao=None):
    """Execuções de uma ferramenta, opcionalmente numa condição."""
    return [r for r in execucoes if r["execucao"]["ferramenta"] == ferramenta
            and (condicao is None or r["execucao"]["condicao"] == condicao)]


def cpu(resumo, chave="recursos_backend"):
    """CPU somada dos contêineres, em porcentagem de um núcleo.

    No Jaeger e no Zipkin, soma o servidor e o Elasticsearch que guarda os spans.
    """
    return sum(v["cpu_pct"] or 0 for v in resumo.get(chave, {}).values())


def memoria(resumo, chave="recursos_backend"):
    """Memória mediana somada dos contêineres, em MiB."""
    return sum(v["memoria_mediana_mib"] for v in resumo.get(chave, {}).values())


def mediana(valores):
    return statistics.median(valores) if valores else float("nan")


def quartis(valores):
    """Primeiro e terceiro quartis, para as faixas das figuras."""
    return tuple(np.percentile(valores, [25, 75])) if valores else (np.nan, np.nan)


# ---------------------------------------------------------------- comparação e tabelas

def intervalo_razao(referencia, comparada, confianca=0.95):
    """Intervalo de confiança da razão entre as medianas, por bootstrap de percentis.

    Sorteia, com reposição, novas amostras de cada ferramenta do mesmo tamanho das originais
    e calcula a razão das medianas em cada sorteio. Os percentis 2,5 e 97,5 dessas razões
    formam o intervalo de 95%. A semente fixa faz o resultado ser o mesmo a cada execução.
    """
    sorteio = np.random.default_rng(0)
    a, b = np.asarray(referencia, float), np.asarray(comparada, float)
    ma = np.median(sorteio.choice(a, (REAMOSTRAGENS, len(a))), axis=1)
    mb = np.median(sorteio.choice(b, (REAMOSTRAGENS, len(b))), axis=1)
    validas = ma != 0
    razoes = mb[validas] / ma[validas]
    if not len(razoes):
        return float("nan"), float("nan")
    cauda = (1 - confianca) / 2 * 100
    return tuple(np.percentile(razoes, [cauda, 100 - cauda]))


def comparar(referencia, comparada):
    """Medianas, razão com intervalo de 95% e p-valor do Mann-Whitney entre as duas listas."""
    linha = {"mediana_referencia": mediana(referencia), "mediana_comparada": mediana(comparada),
             "n_referencia": len(referencia), "n_comparada": len(comparada),
             "razao": float("nan"), "ic_inferior": float("nan"), "ic_superior": float("nan"),
             "p": float("nan")}
    if linha["mediana_referencia"]:
        linha["razao"] = linha["mediana_comparada"] / linha["mediana_referencia"]
        if len(referencia) >= 2 and len(comparada) >= 2:
            linha["ic_inferior"], linha["ic_superior"] = intervalo_razao(referencia, comparada)
    # com todos os valores iguais nas duas listas, o teste não se aplica
    if len(referencia) >= 2 and len(comparada) >= 2 and len(set(referencia + comparada)) > 1:
        linha["p"] = mannwhitneyu(comparada, referencia, alternative="two-sided").pvalue
    return linha


class Tabela:
    """Linhas de comparação entre as ferramentas de cada par, gravadas em CSV e em LaTeX.

    No LaTeX, a primeira coluna traz só o nome do par, para a tabela caber na largura do
    texto. A legenda da tabela na monografia deve dizer que a referência é Loki, Prometheus
    e Jaeger, e a comparada, Elasticsearch, VictoriaMetrics e Zipkin.
    """

    def __init__(self, nome, coluna_condicao=None):
        """nome é o nome dos arquivos, e coluna_condicao o título da coluna de condição."""
        self.nome = nome
        self.coluna_condicao = coluna_condicao
        self.linhas = []

    def comparar(self, par, medida, referencia, comparada, condicao="", casas=1, nota="",
                 resumo=None):
        """Acrescenta uma linha comparando as duas ferramentas do par numa medida.

        resumo, quando dado, é o rótulo com que a comparação entra na figura-resumo.
        """
        a, b = PARES[par]
        linha = {"par": par, "comparacao": f"{NOME[b]} / {NOME[a]}", "condicao": condicao,
                 "medida": medida, "casas": casas, "nota": nota,
                 **comparar(referencia, comparada)}
        self.linhas.append(linha)
        if resumo:
            RESUMO.append({**linha, "rotulo": resumo})

    def gravar(self, pasta):
        """Grava o CSV com os números brutos e o .tex com o ambiente tabular formatado."""
        campos = ["par", "comparacao", "condicao", "medida", "mediana_referencia",
                  "mediana_comparada", "razao", "ic_inferior", "ic_superior", "p",
                  "n_referencia", "n_comparada", "nota"]
        with open(pasta / f"{self.nome}.csv", "w", newline="") as arquivo:
            saida = csv.DictWriter(arquivo, fieldnames=campos, extrasaction="ignore")
            saida.writeheader()
            saida.writerows(self.linhas)

        condicao = self.coluna_condicao is not None
        cabecalho = (["Par"] + ([self.coluna_condicao] if condicao else [])
                     + ["Medida", "Referência", "Comparada", "Razão (intervalo de 95\\%)",
                        "p-valor"])
        colunas = "l" + ("l" if condicao else "") + "l" + "rr" + "c" + "r"
        # o espaço entre colunas menor que o padrão (6pt) faz as tabelas caberem em 16 cm
        texto = ["{\\setlength{\\tabcolsep}{3.5pt}", f"\\begin{{tabular}}{{{colunas}}}",
                 "\\toprule",
                 " & ".join(cabecalho) + " \\\\", "\\midrule"]
        anterior = None
        for linha in self.linhas:
            primeira = linha["par"] != anterior   # o nome da comparação só na primeira linha
            if anterior is not None and primeira:
                texto.append("\\midrule")
            anterior = linha["par"]
            significativo = not np.isnan(linha["p"]) and linha["p"] < ALFA
            razao = numero(linha["razao"], 2)
            p = "--" if np.isnan(linha["p"]) else valor_p(linha["p"])
            if significativo:   # resultados significativos em negrito
                razao, p = f"\\textbf{{{razao}}}", f"\\textbf{{{p}}}"
            if not np.isnan(linha["ic_inferior"]):   # o intervalo entre parênteses, após a razão
                razao += (f" ({numero(linha['ic_inferior'], 2)} a "
                          f"{numero(linha['ic_superior'], 2)})")
            medida = latex(linha["medida"]) + (f"\\textsuperscript{{{linha['nota']}}}"
                                               if linha["nota"] else "")
            celulas = ([NOME_PAR[linha["par"]] if primeira else ""]
                       + ([latex(linha["condicao"])] if condicao else [])
                       + [medida, numero(linha["mediana_referencia"], linha["casas"]),
                          numero(linha["mediana_comparada"], linha["casas"]), razao, p])
            texto.append(" & ".join(celulas) + " \\\\")
        texto += ["\\bottomrule", "\\end{tabular}}", ""]
        (pasta / f"{self.nome}.tex").write_text("\n".join(texto))


# ---------------------------------------------------------------- figuras auxiliares

def paineis(linhas, colunas, altura):
    """Figura com a largura do texto e os painéis em grade."""
    figura, eixos = plt.subplots(linhas, colunas, figsize=(LARGURA, altura), squeeze=False)
    return figura, eixos


def caixas(eixo, grupos, posicoes, cores, rotulos=None):
    """Boxplot com os pontos de cada repetição por cima.

    Com 10 repetições, os pontos mostram a distribuição real, e a caixa resume a mediana e
    os quartis. Os valores atípicos não são desenhados à parte, porque já aparecem como pontos.
    """
    sorteio = np.random.default_rng(0)   # deslocamento horizontal fixo entre execuções
    for valores, x, c in zip(grupos, posicoes, cores):
        if not valores:
            continue
        eixo.boxplot([valores], positions=[x], widths=0.5, showfliers=False,
                     medianprops={"color": c, "linewidth": 1.6},
                     boxprops={"color": CINZA, "linewidth": 0.7},
                     whiskerprops={"color": CINZA, "linewidth": 0.7},
                     capprops={"color": CINZA, "linewidth": 0.7})
        deslocamento = sorteio.uniform(-0.12, 0.12, len(valores))
        eixo.scatter(np.full(len(valores), x) + deslocamento, valores, s=14, color=c,
                     edgecolor="white", linewidth=0.6, zorder=3)
    if rotulos:
        eixo.set_xticks(posicoes)
        eixo.set_xticklabels(rotulos)


def legenda_par(eixo, par, abaixo=False):
    """Legenda com as duas ferramentas do par, nas cores delas.

    Com abaixo, a legenda fica sob o eixo, para não cobrir dados que ocupam todo o painel.
    """
    from matplotlib.patches import Patch
    marcas = [Patch(color=cor(f), label=NOME[f]) for f in PARES[par]]
    if abaixo:
        eixo.legend(handles=marcas, loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=2)
    else:
        eixo.legend(handles=marcas, loc="upper left")


def salvar(figura, pasta, nome):
    """Grava a figura em PDF e fecha."""
    figura.tight_layout()
    figura.savefig(pasta / f"{nome}.pdf")
    plt.close(figura)


# ---------------------------------------------------------------- saturação

def ler_degraus(resumo):
    """Taxa pedida e aceita de cada degrau, lidas no degraus.csv da execução."""
    arquivo = resumo["pasta"] / "degraus.csv"
    if not arquivo.exists():
        return []
    with open(arquivo) as f:
        return [(float(l["alvo"]), float(l["aceita"]), l["estado"]) for l in csv.DictReader(f)]


def saturacao(execucoes, figuras, tabelas):
    """Curva de saturação e tabela da vazão máxima."""
    figura, eixos = paineis(1, 3, 2.4)
    tabela = Tabela("saturacao")
    for eixo, (par, ferramentas) in zip(eixos[0], PARES.items()):
        escala, unidade = (1e6, "milhões") if par == "metricas" else (1e3, "mil")
        maximo = 0
        vazoes = {}
        for f in ferramentas:
            vazoes[f] = []
            for r in filtrar(execucoes, f):
                degraus = ler_degraus(r)
                if degraus:
                    x = [d[0] / escala for d in degraus]
                    y = [d[1] / escala for d in degraus]
                    eixo.plot(x, y, color=cor(f), alpha=0.55, linewidth=0.9, marker="o",
                              markersize=2.5)
                    maximo = max(maximo, *x, *y)
                    # o limite do gerador fica marcado no ponto em que a execução parou
                    if degraus[-1][2] == "gerador_limitado":
                        eixo.plot(x[-1], y[-1], marker="^", color=cor(f), markersize=5,
                                  markeredgecolor="white", markeredgewidth=0.5, zorder=4)
                vazao = (r.get("saturacao") or {}).get("vazao_maxima")
                if vazao is not None:
                    vazoes[f].append(vazao)
        eixo.plot([0, maximo * 1.05], [0, maximo * 1.05], color=CINZA, linewidth=0.7,
                  linestyle="--", zorder=0)   # diagonal: tudo o que foi pedido foi aceito
        eixo.set_xlim(0, maximo * 1.05)
        eixo.set_ylim(0, maximo * 1.05)
        eixo.set_title(f"{NOME_PAR[par]} ({unidade} de {ITEM[par]})")
        eixo.set_xlabel("Taxa pedida")
        if par == "logs":
            eixo.set_ylabel("Taxa aceita")
        eixo_brasileiro(eixo)
        legenda_par(eixo, par)
        # a comparada do par de métricas parou por limite do gerador: só limite inferior
        paradas = [(r.get("saturacao") or {}).get("parada") for r in filtrar(execucoes, ferramentas[1])]
        nota = "a" if paradas and paradas.count("gerador_limitado") > len(paradas) / 2 else ""
        tabela.comparar(par, f"Vazão máxima ({ITEM[par]})", vazoes[ferramentas[0]],
                        vazoes[ferramentas[1]], casas=0, nota=nota,
                        resumo="Vazão máxima (maior é melhor)" + (", limite inferior" if nota else ""))
    salvar(figura, figuras, "saturacao_curva")
    tabela.gravar(tabelas)


# ---------------------------------------------------------------- ingestão

def ingestao(execucoes, figuras, tabelas):
    """Linhas de CPU e memória por nível, barras de backend e Collector e a tabela."""
    tabela = Tabela("ingestao", "Nível")
    figura, eixos = paineis(2, 3, 4.4)
    for coluna, (par, ferramentas) in enumerate(PARES.items()):
        for linha, (medida, funcao, divisor) in enumerate((("CPU (%)", cpu, 1),
                                                            ("Memória (GiB)", memoria, 1024))):
            eixo = eixos[linha][coluna]
            for f in ferramentas:
                valores = [[funcao(r) / divisor for r in filtrar(execucoes, f, f"{n}pct")]
                           for n in NIVEIS]
                if not any(valores):
                    continue
                meio = [mediana(v) for v in valores]
                baixo, alto = zip(*[quartis(v) for v in valores])
                eixo.plot(NIVEIS, meio, color=cor(f), marker="o", markersize=4, label=NOME[f])
                eixo.fill_between(NIVEIS, baixo, alto, color=cor(f), alpha=0.18, linewidth=0)
            eixo.set_xticks(NIVEIS)
            eixo.set_ylim(bottom=0)
            if linha == 0:
                eixo.set_title(NOME_PAR[par])
                legenda_par(eixo, par)
            else:
                eixo.set_xlabel("Nível de carga (% de S)")
            if coluna == 0:
                eixo.set_ylabel(medida)
            eixo_brasileiro(eixo)
        # tabela: backend em todos os pares e Collector nos pares que passam por ele
        a, b = ferramentas
        for n in NIVEIS:
            ra, rb = filtrar(execucoes, a, f"{n}pct"), filtrar(execucoes, b, f"{n}pct")
            nivel = f"{n}%"
            destaque = n == NIVEIS[-1]   # o nível mais alto entra na figura-resumo
            tabela.comparar(par, "CPU (%)", [cpu(r) for r in ra],
                            [cpu(r) for r in rb], nivel,
                            resumo=destaque and f"CPU do backend, ingestão a {n}%")
            tabela.comparar(par, "Memória (MiB)", [memoria(r) for r in ra],
                            [memoria(r) for r in rb], nivel, casas=0,
                            resumo=destaque and f"Memória do backend, ingestão a {n}%")
            if par != "metricas":
                tabela.comparar(par, "CPU do Collector (%)",
                                [cpu(r, "recursos_collector") for r in ra],
                                [cpu(r, "recursos_collector") for r in rb], nivel)
                tabela.comparar(par, "CPU somada (%)",
                                [cpu(r) + cpu(r, "recursos_collector") for r in ra],
                                [cpu(r) + cpu(r, "recursos_collector") for r in rb], nivel,
                                resumo=destaque and f"CPU com o Collector, ingestão a {n}%")
    salvar(figura, figuras, "ingestao_recursos")
    tabela.gravar(tabelas)
    ingestao_collector(execucoes, figuras)


def ingestao_collector(execucoes, figuras, nivel=75):
    """Barras empilhadas de backend e Collector no nível mais alto, nos pares com Collector.

    O segmento do Collector leva hachura, para que a divisão seja legível também em
    impressão em preto e branco.
    """
    figura, eixos = paineis(1, 4, 2.3)
    pares = [p for p in PARES if p != "metricas"]
    medidas = (("CPU (%)", cpu, 1), ("Memória (GiB)", memoria, 1024))
    for indice, (par, (medida, funcao, divisor)) in enumerate(
            [(p, m) for p in pares for m in medidas]):
        eixo = eixos[0][indice]
        for x, f in enumerate(PARES[par]):
            rs = filtrar(execucoes, f, f"{nivel}pct")
            if not rs:
                continue
            backend = mediana([funcao(r) / divisor for r in rs])
            coletor = mediana([funcao(r, "recursos_collector") / divisor for r in rs])
            eixo.bar(x, backend, width=0.6, color=cor(f), edgecolor="white", linewidth=1)
            eixo.bar(x, coletor, bottom=backend, width=0.6, color="white", edgecolor=cor(f),
                     hatch="////", linewidth=0.8)
        eixo.set_xticks([0, 1])
        eixo.set_xticklabels([NOME[f] for f in PARES[par]], rotation=15)
        eixo.set_title(f"{NOME_PAR[par]}, {medida}")
        eixo_brasileiro(eixo, x=False)
    from matplotlib.patches import Patch
    figura.legend(handles=[Patch(facecolor=CINZA, label="Backend"),
                           Patch(facecolor="white", edgecolor=CINZA, hatch="////",
                                 label="Collector")],
                  loc="upper center", ncol=2, bbox_to_anchor=(0.5, 1.08))
    salvar(figura, figuras, "ingestao_collector")


# ---------------------------------------------------------------- cardinalidade

def cardinalidade(execucoes, figuras, tabelas):
    """Linhas de CPU e memória por número de séries e a tabela por patamar."""
    tabela = Tabela("cardinalidade", "Séries")
    figura, eixos = paineis(1, 2, 2.4)
    a, b = PARES["metricas"]
    x = [series / 1e3 for _, series in PATAMARES]
    for eixo, (medida, funcao, divisor) in zip(eixos[0], (("CPU (%)", cpu, 1),
                                                           ("Memória (GiB)", memoria, 1024))):
        for f in (a, b):
            valores = [[funcao(r) / divisor for r in filtrar(execucoes, f, nome)]
                       for nome, _ in PATAMARES]
            if not any(valores):
                continue
            baixo, alto = zip(*[quartis(v) for v in valores])
            eixo.plot(x, [mediana(v) for v in valores], color=cor(f), marker="o",
                      markersize=4, label=NOME[f])
            eixo.fill_between(x, baixo, alto, color=cor(f), alpha=0.18, linewidth=0)
        eixo.set_xticks(x)
        eixo.set_xlim(0, 1050)
        eixo.set_ylim(bottom=0)
        eixo.set_xlabel("Séries (mil)")
        eixo.set_ylabel(medida)
        eixo_brasileiro(eixo)
        legenda_par(eixo, "metricas")
    for nome, series in PATAMARES:
        ra, rb = filtrar(execucoes, a, nome), filtrar(execucoes, b, nome)
        rotulo = NOME_PATAMAR[series]
        maior = series == PATAMARES[-1][1]   # o maior patamar entra na figura-resumo
        tabela.comparar("metricas", "CPU (%)", [cpu(r) for r in ra], [cpu(r) for r in rb],
                        rotulo, resumo=maior and f"CPU, cardinalidade de {rotulo}")
        tabela.comparar("metricas", "Memória (MiB)", [memoria(r) for r in ra],
                        [memoria(r) for r in rb], rotulo, casas=0,
                        resumo=maior and f"Memória, cardinalidade de {rotulo}")
        # memória por série: quanto cada série ativa custa ao backend
        tabela.comparar("metricas", "Bytes por série",
                        [memoria(r) * 2**20 / series for r in ra],
                        [memoria(r) * 2**20 / series for r in rb], rotulo, casas=0)
    salvar(figura, figuras, "cardinalidade")
    tabela.gravar(tabelas)


# ---------------------------------------------------------------- cenário realista

def realista(execucoes, figuras, tabelas):
    """Boxplots de CPU e memória, barras de bytes por item e a tabela."""
    tabela = Tabela("realista")
    figura, eixos = paineis(2, 3, 4.4)
    for coluna, (par, ferramentas) in enumerate(PARES.items()):
        for linha, (medida, funcao, divisor) in enumerate((("CPU (%)", cpu, 1),
                                                            ("Memória (GiB)", memoria, 1024))):
            eixo = eixos[linha][coluna]
            grupos = [[funcao(r) / divisor for r in filtrar(execucoes, f)] for f in ferramentas]
            caixas(eixo, grupos, [0, 1], [cor(f) for f in ferramentas],
                   [NOME[f] for f in ferramentas])
            eixo.set_xlim(-0.6, 1.6)
            eixo.set_ylim(bottom=0)
            if linha == 0:
                eixo.set_title(NOME_PAR[par])
            if coluna == 0:
                eixo.set_ylabel(medida)
            eixo_brasileiro(eixo, x=False)
        ra, rb = filtrar(execucoes, ferramentas[0]), filtrar(execucoes, ferramentas[1])
        tabela.comparar(par, "CPU (%)", [cpu(r) for r in ra], [cpu(r) for r in rb],
                        resumo="CPU, cenário realista")
        tabela.comparar(par, "Memória (MiB)", [memoria(r) for r in ra],
                        [memoria(r) for r in rb], casas=0, resumo="Memória, cenário realista")
        tabela.comparar(par, "Disco por item (bytes)",
                        [r["bytes_por_item"] for r in ra if r.get("bytes_por_item")],
                        [r["bytes_por_item"] for r in rb if r.get("bytes_por_item")],
                        casas=2 if par == "metricas" else 0,
                        resumo="Disco por item, cenário realista")
    salvar(figura, figuras, "realista_recursos")
    tabela.gravar(tabelas)

    # disco por item: barra na mediana e os pontos de cada repetição
    figura, eixos = paineis(1, 3, 2.3)
    for eixo, (par, ferramentas) in zip(eixos[0], PARES.items()):
        sorteio = np.random.default_rng(0)
        for x, f in enumerate(ferramentas):
            valores = [r["bytes_por_item"] for r in filtrar(execucoes, f)
                       if r.get("bytes_por_item")]
            if not valores:
                continue
            eixo.bar(x, mediana(valores), width=0.6, color=cor(f), alpha=0.85,
                     edgecolor="white", linewidth=1)
            eixo.scatter(x + sorteio.uniform(-0.12, 0.12, len(valores)), valores, s=12,
                         color=CINZA, edgecolor="white", linewidth=0.5, zorder=3)
        eixo.set_xticks([0, 1])
        eixo.set_xticklabels([NOME[f] for f in ferramentas])
        eixo.set_title(NOME_PAR[par])
        if par == "logs":
            eixo.set_ylabel("Disco por item (bytes)")
        eixo_brasileiro(eixo, x=False)
    salvar(figura, figuras, "realista_disco")


# ---------------------------------------------------------------- consulta

def consulta(execucoes, figuras, tabelas):
    """Boxplots das medianas por execução de cada consulta e a tabela."""
    tabela = Tabela("consulta", "Consulta")
    figura, eixos = paineis(1, 2, 2.6)
    for eixo, (par, consultas) in zip(eixos[0], CONSULTAS.items()):
        a, b = PARES[par]
        posicoes, rotulos = [], []
        for i, (chave, rotulo) in enumerate(consultas):
            grupos = [[r["consultas"][chave]["mediana_ms"] for r in filtrar(execucoes, f)
                       if chave in r.get("consultas", {})] for f in (a, b)]
            caixas(eixo, grupos, [3 * i, 3 * i + 1], [cor(a), cor(b)])
            posicoes.append(3 * i + 0.5)
            rotulos.append(rotulo)
            p95 = [[r["consultas"][chave]["p95_ms"] for r in filtrar(execucoes, f)
                    if chave in r.get("consultas", {})] for f in (a, b)]
            tabela.comparar(par, "Mediana (ms)", grupos[0], grupos[1], rotulo,
                            resumo=(f"Consulta: {rotulo.lower()}" if par == "logs"
                                    else f"Busca por {rotulo.lower()}"))
            tabela.comparar(par, "Percentil 95 (ms)", p95[0], p95[1], rotulo)
        eixo.set_yscale("log")   # as consultas vão de poucos milissegundos a centenas
        eixo.yaxis.set_major_locator(LogLocator(subs=(1, 2, 5)))   # marcas em 1, 2 e 5
        eixo.set_xticks(posicoes)
        eixo.set_xticklabels(rotulos)
        eixo.set_title(NOME_PAR[par])
        eixo.set_ylabel("Tempo de resposta (ms)")
        eixo_brasileiro(eixo, x=False)
        eixo.yaxis.set_minor_formatter(FuncFormatter(lambda *_: ""))
        legenda_par(eixo, par, abaixo=True)
    salvar(figura, figuras, "consultas")
    tabela.gravar(tabelas)


# ---------------------------------------------------------------- figura-resumo

def resumo(figuras):
    """Razões com intervalo de 95% das principais comparações, um painel por par.

    O eixo horizontal é logarítmico, para que uma razão de 2 e uma de 0,5 fiquem à mesma
    distância da linha 1, que marca a ausência de diferença. À direita, a ferramenta
    comparada teve valor maior que a referência. Nos custos, como CPU, memória, disco e
    tempo, isso significa que a comparada gastou mais. Na vazão, que a comparada aguentou
    mais. Pontos cheios são diferenças significativas, e pontos vazados, não significativas.
    """
    pares = [p for p in PARES if any(l["par"] == p for l in RESUMO)]
    if not pares:
        return
    alturas = [sum(l["par"] == p for l in RESUMO) for p in pares]
    figura, eixos = plt.subplots(len(pares), 1, sharex=True,
                                 figsize=(LARGURA, 0.21 * sum(alturas) + 1.2),
                                 gridspec_kw={"height_ratios": alturas})
    eixos = np.atleast_1d(eixos)
    todas = [v for l in RESUMO for v in (l["ic_inferior"], l["ic_superior"], l["razao"])
             if v and not np.isnan(v)]
    limite = max(abs(np.log10(min(todas))), abs(np.log10(max(todas)))) * 1.15
    for eixo, par in zip(eixos, pares):
        linhas = [l for l in RESUMO if l["par"] == par]
        y = np.arange(len(linhas))[::-1]
        for yi, l in zip(y, linhas):
            if np.isnan(l["razao"]):
                continue
            significativo = not np.isnan(l["p"]) and l["p"] < ALFA
            eixo.plot([l["ic_inferior"], l["ic_superior"]], [yi, yi], color=CINZA, linewidth=1.2)
            eixo.plot(l["razao"], yi, marker="o", markersize=5, color=CINZA,
                      markerfacecolor=CINZA if significativo else "white", markeredgewidth=1.1)
        eixo.axvline(1, color="#0b0b0b", linewidth=0.9, zorder=1)   # sem diferença
        eixo.set_yticks(y)
        eixo.set_yticklabels([l["rotulo"] for l in linhas])
        eixo.set_ylim(-0.7, len(linhas) - 0.3)
        eixo.grid(axis="x", color="#e3e2de", linewidth=0.5)
        eixo.grid(axis="y", visible=False)
        a, b = PARES[par]
        eixo.set_title(f"{NOME_PAR[par]}: {NOME[b]} em relação ao {NOME[a]}", loc="left")
    eixo = eixos[-1]
    eixo.set_xscale("log")
    eixo.set_xlim(10 ** -limite, 10 ** limite)
    eixo.xaxis.set_major_locator(LogLocator(subs=(1, 2, 5)))
    eixo.xaxis.set_major_formatter(FuncFormatter(_marca))
    eixo.xaxis.set_minor_formatter(FuncFormatter(lambda *_: ""))
    # o sentido do eixo, dos dois lados da linha 1, logo abaixo das marcas
    for texto, deslocamento, alinhamento in (("← comparada menor", -6, "right"),
                                             ("comparada maior →", 6, "left")):
        eixo.annotate(texto, xy=(1, 0), xycoords=("data", "axes fraction"),
                      xytext=(deslocamento, -20), textcoords="offset points",
                      ha=alinhamento, va="top", fontsize=8, color=CINZA)
    eixo.set_xlabel("Razão entre as medianas (comparada / referência), com intervalo de 95%",
                    labelpad=18)
    salvar(figura, figuras, "resumo")


# ---------------------------------------------------------------- verificação

ESPERADO = {"saturacao": 6, "ingestao": 18, "cardinalidade": 6, "realista": 6, "consulta": 4}


def verificacao(execucoes, tabelas):
    """Lista as execuções que merecem atenção e confere se todas as repetições existem."""
    problemas = []
    for experimento, lista in execucoes.items():
        for r in lista:
            e = r["execucao"]
            avisos = []
            perda = r.get("perda_real_pct", r.get("perda_pct"))
            if perda:
                avisos.append(f"perda de {numero(perda, 3)}%")
            if (r.get("carga") or {}).get("sustentada") is False:
                avisos.append("carga não sustentada")
            if r.get("memoria_esgotada"):
                avisos.append("memória esgotada em " + ", ".join(r["memoria_esgotada"]))
            if (r.get("saturacao") or {}).get("parada") == "gerador_limitado":
                avisos.append("parada pelo limite do gerador")
            if r.get("series_confirmadas") is False:
                avisos.append("número de séries não confere")
            if (r.get("aquecimento") or {}).get("incompleto"):
                avisos.append("aquecimento incompleto")
            for nome, c in (r.get("consultas") or {}).items():
                if c.get("falhas"):
                    avisos.append(f"{c['falhas']} falhas em {nome}")
                if c.get("variacao_pct") is not None and abs(c["variacao_pct"]) > 5:
                    avisos.append(f"variação de {numero(c['variacao_pct'])}% em {nome}")
            if avisos:
                problemas.append({"experimento": experimento, "ferramenta": e["ferramenta"],
                                  "condicao": e["condicao"], "repeticao": e["repeticao"],
                                  "avisos": "; ".join(avisos)})
    with open(tabelas / "verificacao.csv", "w", newline="") as arquivo:
        saida = csv.DictWriter(arquivo, fieldnames=["experimento", "ferramenta", "condicao",
                                                    "repeticao", "avisos"])
        saida.writeheader()
        saida.writerows(problemas)

    print("execuções encontradas por experimento:")
    for experimento, combinacoes in ESPERADO.items():
        lista = execucoes.get(experimento, [])
        grupos = {}
        for r in lista:
            e = r["execucao"]
            grupos.setdefault((e["ferramenta"], e["condicao"]), []).append(e["repeticao"])
        repeticoes = sorted({len(v) for v in grupos.values()}) or [0]
        print(f"  {experimento:<14} {len(lista):>4} execuções, {len(grupos)} de {combinacoes} "
              f"combinações, de {repeticoes[0]} a {repeticoes[-1]} repetições cada")
    print(f"{len(problemas)} execuções com avisos, ver verificacao.csv")


# ---------------------------------------------------------------- principal

def main():
    """Lê os resultados e gera todas as tabelas e figuras."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("resultados", nargs="?", default="resultados")
    parser.add_argument("--saida", default=str(Path(__file__).resolve().parent))
    args = parser.parse_args()
    execucoes = carregar(args.resultados)
    if not execucoes:
        sys.exit(f"nenhum resumo.json encontrado em {args.resultados}")
    figuras, tabelas = Path(args.saida) / "figuras", Path(args.saida) / "tabelas"
    figuras.mkdir(parents=True, exist_ok=True)
    tabelas.mkdir(parents=True, exist_ok=True)
    estilo()
    etapas = {"saturacao": saturacao, "ingestao": ingestao, "cardinalidade": cardinalidade,
              "realista": realista, "consulta": consulta}
    for experimento, funcao in etapas.items():
        if experimento in execucoes:
            funcao(execucoes[experimento], figuras, tabelas)
    resumo(figuras)
    verificacao(execucoes, tabelas)
    print(f"tabelas em {tabelas} e figuras em {figuras}")


if __name__ == "__main__":
    main()
