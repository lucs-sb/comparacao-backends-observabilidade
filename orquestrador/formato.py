"""Formato dos itens sintéticos, medido na OpenTelemetry Demo 3.1.0 (P6, A17).

Na Demo, o corpo dos logs tem em média 207 bytes, os logs têm 7 atributos, os spans têm 8,
os recursos têm 17 atributos e os traces têm em média 5,1 spans. Os geradores reproduzem
esses valores e dividem a carga entre 8 serviços, como uma aplicação de microsserviços.
"""
import math

SERVICOS = ["frontend-proxy", "frontend", "product-catalog", "cart",
            "recommendation", "currency", "checkout", "shipping"]
POR_WORKER = 1000   # taxa máxima por worker do telemetrygen; acima disso, mais workers
FILHOS = 4          # 1 span pai e 4 filhos, perto da média de 5,1 spans por trace

# corpo de 200 bytes no formato de um log de acesso, perto da média de 207 bytes da Demo
CORPO_LOG = (
    '10.0.4.17 - - [28/Oct/2026:17:40:12 +0000] "GET /api/products/OLJCESPC7Z HTTP/1.1" '
    '200 1534 "-" "python-requests/2.32.3" rt=0.012 upstream=product-catalog:3550 '
    'request_id=4f9c2a7e81d34b6f9a0c5e2d7b18f3a6c4e9d0b2a7f15c83e6d4b9a2f0c7e1d5b'
)[:200]

ATRIBUTOS_LOG = [
    'http.request.method="GET"',
    'http.response.status_code="200"',
    'http.route="/api/products/{id}"',
    'url.path="/api/products/OLJCESPC7Z"',
    'server.address="frontend-proxy"',
    'network.protocol.version="1.1"',
    'user_agent.original="python-requests/2.32.3"',
]
ATRIBUTOS_SPAN = ATRIBUTOS_LOG + ['app.product.id="OLJCESPC7Z"']

# 16 atributos, mais o service.name definido por --service, somam os 17 da Demo
ATRIBUTOS_RECURSO = [
    'service.namespace="opentelemetry-demo"',
    'service.version="3.1.0"',
    'deployment.environment.name="experimento"',
    'host.name="tcc-carga"',
    'host.arch="amd64"',
    'os.type="linux"',
    'os.description="Ubuntu 26.04 LTS"',
    'process.pid="1"',
    'process.executable.name="telemetrygen"',
    'process.runtime.name="go"',
    'process.runtime.version="go1.24"',
    'telemetry.sdk.name="opentelemetry"',
    'telemetry.sdk.language="go"',
    'telemetry.sdk.version="1.38.0"',
    'container.id="tcc-gerador"',
    'cloud.provider="aws"',
]


def argumentos(par, servico, taxa, duracao):
    """Argumentos do telemetrygen para um serviço, com a taxa em itens por segundo.

    Cada worker envia um item por vez e espera a resposta do Collector, então o número
    de workers cresce com a taxa, e nenhum deles passa de POR_WORKER itens por segundo.
    No par de traces, a taxa do telemetrygen conta spans (A1).
    """
    workers = max(1, math.ceil(taxa / POR_WORKER))
    por_worker = max(1, round(taxa / workers))
    comum = ["--rate", str(por_worker), "--workers", str(workers),
             "--duration", f"{duracao}s", "--service", servico]
    for atributo in ATRIBUTOS_RECURSO:
        comum += ["--otlp-attributes", atributo]
    if par == "logs":
        args = ["logs", *comum, "--body", CORPO_LOG]
        atributos = ATRIBUTOS_LOG
    else:
        args = ["traces", *comum, "--child-spans", str(FILHOS)]
        atributos = ATRIBUTOS_SPAN
    for atributo in atributos:
        args += ["--telemetry-attributes", atributo]
    return args
