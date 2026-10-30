"""Divide a saída do flog nos arquivos fixos do experimento de consulta (P7).

Uso, na instância 2 (o orquestrador chama sozinho quando os arquivos não existem):
    docker run --rm <imagem do flog> -f apache_combined -n 1000000 -t stdout \
        | python3 scripts/gerar_logs_consulta.py <pasta> 100 marcadortcc

As linhas vão em blocos contínuos para os arquivos svc-000.log a svc-099.log, cada um um
serviço com 1% dos logs. Uma linha em cada 100 recebe o termo marcador no fim, então a busca
textual também seleciona 1% dos logs, espalhados por todos os serviços.
"""
import sys
from pathlib import Path

pasta, servicos, termo = Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
linhas = [l.rstrip("\n") for l in sys.stdin if l.strip()]
por_servico = len(linhas) // servicos
pasta.mkdir(parents=True, exist_ok=True)
for s in range(servicos):
    with open(pasta / f"svc-{s:03d}.log", "w") as arquivo:
        for i in range(s * por_servico, (s + 1) * por_servico):
            linha = linhas[i] + (f" {termo}" if i % 100 == 0 else "")
            arquivo.write(linha + "\n")
(pasta / "pronto").write_text(f"{servicos * por_servico}\n")
print(f"{servicos * por_servico} linhas em {servicos} arquivos, "
      f"{servicos * por_servico // 100} com o termo {termo}")
