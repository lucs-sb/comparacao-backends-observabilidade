"""Caminhos do repositório e versões lidas de versoes.env."""
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
ARQUIVO_VERSOES = RAIZ / "versoes.env"


def _ler_versoes():
    versoes = {}
    for linha in ARQUIVO_VERSOES.read_text().splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, valor = linha.split("=", 1)
        versoes[chave.strip()] = valor.split("#", 1)[0].strip()
    return versoes


VERSOES = _ler_versoes()
