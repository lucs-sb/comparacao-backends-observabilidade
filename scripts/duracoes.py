"""Duração de cada execução já feita, para estimar o tempo do experimento completo (P11).

Uso, na raiz do repositório:
    python3 scripts/duracoes.py [resultados]

Cada execução tem uma pasta com um resumo.json. A duração total vai da criação da pasta até a
última gravação do resumo, o que inclui limpar o ambiente, subir o backend, aplicar a carga,
descarregar, contar e medir. Quando o resumo traz o início e o fim da carga, a duração da carga
aparece separada, e a diferença é o custo fixo de preparar e encerrar a execução.
"""
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path


def criacao(pasta):
    """Momento de criação da pasta, pelo stat do sistema de arquivos."""
    saida = subprocess.run(["stat", "-c", "%W", str(pasta)], capture_output=True, text=True)
    valor = int(saida.stdout.strip() or 0)
    if valor > 0:
        return valor
    # Sem a data de criação, usa o arquivo mais antigo da pasta
    return min(f.stat().st_mtime for f in pasta.iterdir())


def main():
    raiz = Path(sys.argv[1] if len(sys.argv) > 1 else "resultados")
    linhas = []
    for resumo in sorted(raiz.rglob("resumo.json")):
        pasta = resumo.parent
        total = resumo.stat().st_mtime - criacao(pasta)
        try:
            dados = json.load(open(resumo))
        except (json.JSONDecodeError, OSError):
            dados = {}
        carga = None
        if dados.get("inicio_carga") and dados.get("fim_carga"):
            carga = dados["fim_carga"] - dados["inicio_carga"]
        linhas.append((str(pasta.relative_to(raiz)), total, carga))

    largura = max((len(l[0]) for l in linhas), default=10)
    print(f"{'execução':<{largura}}  {'total':>7}  {'carga':>7}  {'fixo':>7}   (minutos)")
    for nome, total, carga in linhas:
        c = f"{carga / 60:7.1f}" if carga is not None else "      -"
        f = f"{(total - carga) / 60:7.1f}" if carga is not None else "      -"
        print(f"{nome:<{largura}}  {total / 60:7.1f}  {c}  {f}")

    # Média por experimento, par e ferramenta, ignorando a condição e a repetição
    grupos = {}
    for nome, total, _ in linhas:
        partes = nome.split(os.sep)
        grupos.setdefault(os.sep.join(partes[:3]), []).append(total)
    print(f"\n{'experimento/par/ferramenta':<{largura}}  {'execuções':>9}  {'média':>7}  {'soma':>7}")
    for grupo, totais in grupos.items():
        print(f"{grupo:<{largura}}  {len(totais):>9}  {statistics.mean(totais) / 60:7.1f}  "
              f"{sum(totais) / 60:7.1f}")


if __name__ == "__main__":
    main()
