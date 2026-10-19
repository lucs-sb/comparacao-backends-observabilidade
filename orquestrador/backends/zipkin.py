from backends.jaeger import Jaeger


class Zipkin(Jaeger):
    """Mesmo arranjo do Jaeger, sobre o mesmo Elasticsearch."""
    pasta = "zipkin"
    url = "http://localhost:9411"
    saude = "/health"
    indice = "zipkin*span*"
