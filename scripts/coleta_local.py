"""Roda a coleta fora do Airflow (teste rápido e geração das amostras).

Uso:  python scripts/coleta_local.py [pasta_destino]
Sem argumento grava em ./chegada (ignorada pelo Git).
"""
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "dags"))

from rua_acesa import coleta  # noqa: E402

destino = Path(sys.argv[1]) if len(sys.argv) > 1 else RAIZ / "chegada"
hoje = date.today()

tarefas = {
    "sp156": lambda: coleta.coletar_sp156(destino / "sp156", hoje),
    "iluminacao": lambda: coleta.coletar_geosampa_iluminacao(destino / "geosampa", hoje),
    "distritos": lambda: coleta.coletar_geosampa_distritos(destino / "geosampa", hoje),
    "openmeteo": lambda: coleta.coletar_openmeteo(destino / "openmeteo", hoje, -23.55, -46.63),
}
with ThreadPoolExecutor(max_workers=4) as ex:
    futuros = {k: ex.submit(f) for k, f in tarefas.items()}
    for k, fu in futuros.items():
        print(k, fu.result())
print("conferência:", coleta.conferir_arquivos(destino, hoje))
