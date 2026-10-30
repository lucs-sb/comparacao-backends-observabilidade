"""Formato dos logs e traces da Demo, a partir dos arquivos do exportador file.

Mede o tamanho do corpo dos logs, o número de atributos e de spans por trace, valores que o
formato.py reproduz nos geradores sintéticos.

Os arquivos vêm da Demo rodando com o Collector configurado para gravar em disco, sem backend.
Para reproduzir, a partir da raiz do repositório, com a Demo já clonada em
demo/opentelemetry-demo (o realista.py clona na primeira execução):

    mkdir -p /tmp/tcc-medicao && chmod 777 /tmp/tcc-medicao
    cp scripts/piloto/formato/extras-medicao.yml \
       demo/opentelemetry-demo/src/otel-collector/otelcol-config-extras.yml
    cd demo/opentelemetry-demo
    docker compose --env-file .env --env-file ../../versoes.env -f compose.yaml \
        -f compose.full.yaml -f ../compose.agente.yml -f ../compose.override.yml \
        -f ../../scripts/piloto/formato/compose.medicao.yml up -d
    # depois de cerca de 10 minutos de carga do Locust da Demo:
    docker compose ... down
    python3 scripts/piloto/formato/analisar_formato.py /tmp/tcc-medicao

Grava também formato.json na mesma pasta. Usa só a biblioteca padrão do Python.
"""
import json
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path


def resumo(valores):
    """Quantidade, média, mediana, percentil 90 e máximo de uma lista de números."""
    if not valores:
        return {}
    ordenados = sorted(valores)
    return {
        "n": len(ordenados),
        "media": round(statistics.fmean(ordenados), 1),
        "mediana": ordenados[len(ordenados) // 2],
        "p90": ordenados[int(len(ordenados) * 0.9)],
        "max": ordenados[-1],
    }


def servico(recurso):
    """Nome do serviço nos atributos de um recurso OTLP, ou ? se faltar."""
    for atributo in recurso.get("attributes", []):
        if atributo["key"] == "service.name":
            return atributo["value"].get("stringValue", "?")
    return "?"


def texto_do_corpo(corpo):
    """Corpo de um log como texto, para medir o tamanho em bytes."""
    if "stringValue" in corpo:
        return corpo["stringValue"]
    return json.dumps(corpo, ensure_ascii=False)  # corpos estruturados contam pelo JSON


def analisar_logs(arquivo):
    """Tamanho do corpo, atributos e serviços dos logs gravados pelo exportador file."""
    corpos, atributos, tamanhos, recursos = [], [], [], []
    servicos, severidades = Counter(), Counter()
    with open(arquivo) as f:
        for linha in f:
            for rl in json.loads(linha).get("resourceLogs", []):
                nome = servico(rl.get("resource", {}))
                n_recurso = len(rl.get("resource", {}).get("attributes", []))
                for sl in rl.get("scopeLogs", []):
                    for log in sl.get("logRecords", []):
                        servicos[nome] += 1
                        severidades[log.get("severityText", "")] += 1
                        corpos.append(len(texto_do_corpo(log.get("body", {})).encode()))
                        atributos.append(len(log.get("attributes", [])))
                        tamanhos.append(len(json.dumps(log, ensure_ascii=False).encode()))
                        recursos.append(n_recurso)
    return {
        "registros": len(corpos),
        "corpo_bytes": resumo(corpos),
        "atributos_por_log": resumo(atributos),
        "atributos_de_recurso": resumo(recursos),
        "registro_json_bytes": resumo(tamanhos),
        "servicos": dict(servicos.most_common()),
        "severidades": dict(severidades.most_common()),
    }


def analisar_traces(arquivo):
    """Spans por trace, serviços por trace, atributos e eventos dos spans gravados."""
    spans_por_trace = Counter()
    servicos_por_trace = defaultdict(set)
    atributos, eventos, tamanhos, duracoes = [], [], [], []
    servicos = Counter()
    with open(arquivo) as f:
        for linha in f:
            for rs in json.loads(linha).get("resourceSpans", []):
                nome = servico(rs.get("resource", {}))
                for ss in rs.get("scopeSpans", []):
                    for span in ss.get("spans", []):
                        spans_por_trace[span["traceId"]] += 1
                        servicos_por_trace[span["traceId"]].add(nome)
                        servicos[nome] += 1
                        atributos.append(len(span.get("attributes", [])))
                        eventos.append(len(span.get("events", [])))
                        tamanhos.append(len(json.dumps(span, ensure_ascii=False).encode()))
                        inicio = int(span.get("startTimeUnixNano", 0))
                        fim = int(span.get("endTimeUnixNano", 0))
                        if fim >= inicio > 0:
                            duracoes.append(round((fim - inicio) / 1e6, 2))  # ms
    return {
        "spans": len(atributos),
        "traces": len(spans_por_trace),
        "spans_por_trace": resumo(list(spans_por_trace.values())),
        "servicos_por_trace": resumo([len(s) for s in servicos_por_trace.values()]),
        "atributos_por_span": resumo(atributos),
        "eventos_por_span": resumo(eventos),
        "span_json_bytes": resumo(tamanhos),
        "duracao_ms": resumo(duracoes),
        "servicos": dict(servicos.most_common()),
    }


def main():
    """Analisa os arquivos da pasta, grava formato.json e mostra o resultado."""
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    pasta = Path(sys.argv[1])
    resultado = {}
    if (pasta / "logs.jsonl").exists():
        resultado["logs"] = analisar_logs(pasta / "logs.jsonl")
    if (pasta / "traces.jsonl").exists():
        resultado["traces"] = analisar_traces(pasta / "traces.jsonl")
    texto = json.dumps(resultado, indent=2, ensure_ascii=False)
    (pasta / "formato.json").write_text(texto)
    print(texto)


if __name__ == "__main__":
    main()
