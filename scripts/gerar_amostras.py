"""Gera amostras pequenas (para o Git) a partir de uma pasta chegada/ já coletada.

Uso:  python scripts/gerar_amostras.py <pasta_chegada> <AAAA-MM-DD>
"""
import csv
import json
import sys
from pathlib import Path

csv.field_size_limit(10**9)
RAIZ = Path(__file__).resolve().parents[1]
chegada, dia = Path(sys.argv[1]), sys.argv[2]


def cabeca_csv(origem: Path, destino: Path, n: int) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(origem, encoding="utf-8", newline="") as f, \
         open(destino, "w", encoding="utf-8", newline="") as g:
        r, w = csv.reader(f), csv.writer(g, lineterminator="\n")
        for i, linha in enumerate(r):
            if i > n:
                break
            w.writerow(linha)


cabeca_csv(chegada / "sp156" / f"sp156_{dia}.csv", RAIZ / "amostras/sp156" / f"sp156_{dia}_amostra.csv", 50)
cabeca_csv(chegada / "geosampa" / f"iluminacao_{dia}.csv", RAIZ / "amostras/geosampa" / f"iluminacao_{dia}_amostra.csv", 200)
cabeca_csv(chegada / "geosampa" / f"distritos_{dia}.csv", RAIZ / "amostras/geosampa" / f"distritos_{dia}_amostra.csv", 2)

js = json.loads((chegada / "openmeteo" / f"openmeteo_{dia}.json").read_text(encoding="utf-8"))
for k in js["daily"]:
    js["daily"][k] = js["daily"][k][:15]
(RAIZ / "amostras/openmeteo").mkdir(parents=True, exist_ok=True)
(RAIZ / "amostras/openmeteo" / f"openmeteo_{dia}_amostra.json").write_text(
    json.dumps(js, ensure_ascii=False, indent=1), encoding="utf-8")
print("amostras geradas em", RAIZ / "amostras")
