"""Funções de coleta do Rua Acesa (Python puro, sem dependência do Airflow).

O DAG (dags/gs_coleta.py) chama estas funções; scripts/coleta_local.py também,
para testar sem subir o Airflow. Todas as fontes são abertas e SEM chave de API.
Cada coleta grava UM arquivo com a data do dia no nome: <fonte>_<AAAA-MM-DD>.<ext>
"""
from __future__ import annotations

import codecs
import csv
import io
import itertools
import json
import re
from datetime import date, timedelta
from pathlib import Path

import requests

UA = {"User-Agent": "rua-acesa-fiap-gs/1.0 (projeto academico)"}

# --- Fonte 1: SP156 (dados abertos, CKAN da Prefeitura) -------------------------
CKAN_PACKAGE_SHOW = "https://dados.prefeitura.sp.gov.br/api/3/action/package_show"
SP156_PACKAGE_ID = "0aecfa2b-aa3a-40d4-8183-0d4351b7fd0a"
SP156_COLUNA_SERVICO = "Serviço"
SP156_COLUNAS_DATA = ["Data de Abertura", "Data de Finalização"]
SP156_SERVICOS_PADRAO = [
    "Denunciar Poste em mau estado",
    "Denunciar Fiação solta na calçada",
]

# --- Fonte 2: GeoSampa (WFS) ------------------------------------------------------
WFS = "http://wfs.geosampa.prefeitura.sp.gov.br/geoserver/ows"
WFS_ILUMINACAO = "geoportal:iluminacao_publica"
WFS_DISTRITOS = "geoportal:distrito_municipal"
COLS_ILUMINACAO = (
    "cd_identificador_iluminacao_publica,dc_tipo_iluminacao_publica,"
    "tx_endereco,ge_ponto"
)
COLS_DISTRITOS = (
    "cd_identificador_distrito,cd_identificador_subprefeitura,cd_distrito_municipal,"
    "nm_distrito_municipal,sg_distrito_municipal,ge_poligono"
)

# --- Fonte 3: Open-Meteo (arquivo histórico, sem chave) ---------------------------
OPENMETEO = "https://archive-api.open-meteo.com/v1/archive"

csv.field_size_limit(10**9)  # polígonos de distrito têm campos WKT enormes


def nome_arquivo(fonte: str, dia: date, ext: str) -> str:
    return f"{fonte}_{dia.isoformat()}.{ext}"


# ----------------------------------------------------------------------------------
# SP156
# ----------------------------------------------------------------------------------
def sp156_recursos_csv(n_trimestres: int = 2) -> list[dict]:
    """Descobre via CKAN os CSVs trimestrais mais recentes ("Dados do SP156 - 3º TRI 2026")."""
    r = requests.get(
        CKAN_PACKAGE_SHOW, params={"id": SP156_PACKAGE_ID}, headers=UA, timeout=60
    )
    r.raise_for_status()
    achados = []
    for rec in r.json()["result"]["resources"]:
        nome = (rec.get("name") or "").strip()
        fmt = (rec.get("format") or "").strip().lower().lstrip(".")
        m = re.match(r"Dados do SP156\s*-\s*(\d)\D{0,3}\s*TRI\s*(\d{4})", nome)
        if fmt == "csv" and m and rec["url"].lower().endswith(".csv"):
            achados.append({"ano": int(m.group(2)), "tri": int(m.group(1)),
                            "nome": nome, "url": rec["url"]})
    achados.sort(key=lambda x: (x["ano"], x["tri"]), reverse=True)
    if not achados:
        raise RuntimeError("Nenhum CSV trimestral do SP156 encontrado no CKAN.")
    return achados[:n_trimestres]


def _detectar_encoding(amostra: bytes) -> str:
    """Os trimestres não têm a mesma codificação (3º tri 2026 é UTF-8; outros, cp1252)."""
    try:
        codecs.getincrementaldecoder("utf-8")().decode(amostra, final=False)
        return "utf-8-sig"
    except UnicodeDecodeError:
        return "cp1252"


def _linhas(resp: requests.Response):
    """Gera as linhas (mantendo a quebra) do corpo HTTP em streaming, detectando a codificação."""
    pedacos = resp.iter_content(chunk_size=1 << 20)
    primeiro = next(pedacos, b"")
    dec = codecs.getincrementaldecoder(_detectar_encoding(primeiro[: 1 << 16]))()
    resto = ""
    for bloco in itertools.chain([primeiro], pedacos):
        resto += dec.decode(bloco)
        linhas = resto.splitlines(keepends=True)
        resto = linhas.pop() if linhas and not linhas[-1].endswith(("\n", "\r")) else ""
        yield from linhas
    resto += dec.decode(b"", final=True)
    if resto:
        yield resto


def _datas_para_iso(linhas: list[list[str]], cols: list[int]) -> None:
    """Converte as datas para AAAA-MM-DD, in-place. O formato muda entre trimestres
    (3º tri/2026: MM/dd/aaaa; 2º tri/2026: dd/MM/aaaa), então é detectado por arquivo:
    se algum 1º campo passa de 12 é dd/MM; se algum 2º campo passa de 12 é MM/dd."""
    partes = [ln[c].split("/") for ln in linhas for c in cols
              if len(ln) > c and ln[c].count("/") == 2]
    dia_primeiro = any(int(p[0]) > 12 for p in partes)
    for ln in linhas:
        for c in cols:
            v = ln[c].strip() if len(ln) > c else ""
            if v.count("/") == 2:
                a, b, ano = v.split("/")
                d, m = (a, b) if dia_primeiro else (b, a)
                ln[c] = f"{ano[:4]}-{int(m):02d}-{int(d):02d}"


def coletar_sp156(destino: Path, dia: date, servicos: list[str] | None = None,
                  n_trimestres: int = 2) -> dict:
    """Baixa em streaming os últimos trimestres (~100 MB cada) e grava só os serviços
    do problema (poste em mau estado / fiação solta). Resultado: poucos milhares de linhas."""
    servicos = set(servicos or SP156_SERVICOS_PADRAO)
    destino.mkdir(parents=True, exist_ok=True)
    arq = destino / nome_arquivo("sp156", dia, "csv")
    cabecalho, linhas, usados = None, 0, []
    with open(arq, "w", encoding="utf-8", newline="") as saida:
        w = csv.writer(saida, lineterminator="\n")
        for rec in sp156_recursos_csv(n_trimestres):
            with requests.get(rec["url"], headers=UA, stream=True, timeout=(30, 600)) as r:
                r.raise_for_status()
                linhas_http = _linhas(r)
                primeira = next(linhas_http)
                # o 2º tri/2026 vem com ';' e o 3º com ',' -> detecta pelo cabeçalho
                sep = ';' if primeira.count(';') > primeira.count(',') else ','
                leitor = csv.reader(itertools.chain([primeira], linhas_http), delimiter=sep)
                cab = next(leitor)
                if cabecalho is None:
                    cabecalho = cab
                    w.writerow(cab)
                elif cab != cabecalho:
                    raise RuntimeError(f"Cabeçalho diferente em {rec['nome']}: {cab}")
                i = cab.index(SP156_COLUNA_SERVICO)
                cols_data = [cab.index(c) for c in SP156_COLUNAS_DATA]
                do_servico = [ln for ln in leitor if len(ln) > i and ln[i].strip() in servicos]
                _datas_para_iso(do_servico, cols_data)
                w.writerows(do_servico)
                linhas += len(do_servico)
            usados.append(rec["nome"])
    return {"arquivo": str(arq), "linhas": linhas, "recursos": usados}


# ----------------------------------------------------------------------------------
# GeoSampa
# ----------------------------------------------------------------------------------
def _wfs_csv_paginado(camada: str, colunas: str, sort_by: str, pagina: int,
                      saida: Path) -> int:
    """Baixa uma camada WFS em CSV (coordenadas em WGS84) paginando por startIndex."""
    total, inicio, primeiro = 0, 0, True
    with open(saida, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        while True:
            r = requests.get(
                WFS,
                params={"service": "WFS", "version": "2.0.0", "request": "GetFeature",
                        "typeNames": camada, "count": pagina, "startIndex": inicio,
                        "srsName": "EPSG:4326", "outputFormat": "csv",
                        "propertyName": colunas, "sortBy": sort_by},
                headers=UA, timeout=(30, 300),
            )
            r.raise_for_status()
            leitor = csv.reader(io.StringIO(r.content.decode("utf-8"), newline=""))
            cab = next(leitor, None)
            if cab is None:
                break
            if primeiro:
                w.writerow(cab)
                primeiro = False
            dados = 0
            for linha in leitor:
                w.writerow(linha)
                dados += 1
            total += dados
            if dados < pagina:
                break
            inicio += pagina
    return total


def coletar_geosampa_iluminacao(destino: Path, dia: date, pagina: int = 20000) -> dict:
    destino.mkdir(parents=True, exist_ok=True)
    arq = destino / nome_arquivo("iluminacao", dia, "csv")
    n = _wfs_csv_paginado(WFS_ILUMINACAO, COLS_ILUMINACAO,
                          "cd_identificador_iluminacao_publica", pagina, arq)
    return {"arquivo": str(arq), "linhas": n}


def coletar_geosampa_distritos(destino: Path, dia: date) -> dict:
    destino.mkdir(parents=True, exist_ok=True)
    arq = destino / nome_arquivo("distritos", dia, "csv")
    n = _wfs_csv_paginado(WFS_DISTRITOS, COLS_DISTRITOS, "cd_identificador_distrito",
                          500, arq)
    return {"arquivo": str(arq), "linhas": n}


# ----------------------------------------------------------------------------------
# Open-Meteo
# ----------------------------------------------------------------------------------
def coletar_openmeteo(destino: Path, dia: date, lat: float, lon: float,
                      dias_historico: int = 120) -> dict:
    """Chuva diária (mm e horas) do arquivo histórico. O arquivo tem defasagem de
    alguns dias, por isso a data final é hoje - 5."""
    destino.mkdir(parents=True, exist_ok=True)
    fim = dia - timedelta(days=5)
    ini = fim - timedelta(days=dias_historico)
    r = requests.get(
        OPENMETEO,
        params={"latitude": lat, "longitude": lon, "start_date": ini.isoformat(),
                "end_date": fim.isoformat(),
                "daily": "precipitation_sum,precipitation_hours",
                "timezone": "America/Sao_Paulo"},
        headers=UA, timeout=60,
    )
    r.raise_for_status()
    corpo = r.json()
    arq = destino / nome_arquivo("openmeteo", dia, "json")
    arq.write_text(json.dumps(corpo, ensure_ascii=False), encoding="utf-8")
    return {"arquivo": str(arq), "dias": len(corpo["daily"]["time"])}


# ----------------------------------------------------------------------------------
# Conferência (tarefa que verifica se os arquivos chegaram)
# ----------------------------------------------------------------------------------
ESPERADOS = {
    # fonte: (subpasta, prefixo, extensão, colunas obrigatórias, mínimo de linhas)
    "sp156": ("sp156", "sp156", "csv",
              ["Data de Abertura", "Serviço", "Status", "Distrito", "Latitude"], 1),
    "iluminacao": ("geosampa", "iluminacao", "csv",
                   ["cd_identificador_iluminacao_publica", "dc_tipo_iluminacao_publica",
                    "ge_ponto"], 100_000),
    "distritos": ("geosampa", "distritos", "csv",
                  ["nm_distrito_municipal", "ge_poligono"], 90),
    "openmeteo": ("openmeteo", "openmeteo", "json", ["daily"], 1),
}


def conferir_arquivos(raiz: Path, dia: date) -> dict:
    """Levanta erro se algum arquivo do dia não existir, estiver vazio ou sem as colunas."""
    resumo, erros = {}, []
    for fonte, (sub, pref, ext, cols, minimo) in ESPERADOS.items():
        arq = raiz / sub / nome_arquivo(pref, dia, ext)
        if not arq.exists() or arq.stat().st_size == 0:
            erros.append(f"{fonte}: arquivo ausente ou vazio ({arq})")
            continue
        if ext == "json":
            corpo = json.loads(arq.read_text(encoding="utf-8"))
            faltam = [c for c in cols if c not in corpo]
            n = len(corpo.get("daily", {}).get("time", []))
        else:
            with open(arq, encoding="utf-8", newline="") as f:
                leitor = csv.reader(f)
                cab = next(leitor, [])
                faltam = [c for c in cols if c not in cab]
                n = sum(1 for _ in leitor)
        if faltam:
            erros.append(f"{fonte}: colunas ausentes {faltam}")
        elif n < minimo:
            erros.append(f"{fonte}: {n} linhas (mínimo {minimo})")
        resumo[fonte] = {"arquivo": arq.name, "linhas": n,
                         "bytes": arq.stat().st_size}
    if erros:
        raise ValueError("Conferência falhou: " + " | ".join(erros))
    return resumo
