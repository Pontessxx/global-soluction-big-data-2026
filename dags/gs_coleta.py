"""DAG gs_coleta - coleta diária do Rua Acesa (Airflow 3).

Fluxo:
    coletar_sp156 ----------\
    coletar_iluminacao -----+--> conferir_arquivos
    coletar_distritos ------|
    coletar_clima ----------/

* As 4 coletas rodam EM PARALELO (requisito 03).
* conferir_arquivos confere existência, colunas e volume mínimo (requisito 03).
* Cada coleta grava UM arquivo com a data no nome em chegada/<fonte>/ (requisito 02).
* Parâmetros ficam em Variables do Airflow (ou variáveis AIRFLOW_VAR_*). Nenhuma das
  fontes usada exige chave de API; se uma for adicionada (ex.: SPTrans), o token
  entra como Variable, nunca no código.
* Falha em qualquer tarefa dispara notificar_falha (log + webhook opcional).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
import requests
from airflow.exceptions import AirflowFailException
from airflow.sdk import DAG, Variable, task

from rua_acesa import coleta

log = logging.getLogger(__name__)
TZ = pendulum.timezone("America/Sao_Paulo")


def _var(nome: str, padrao: str) -> str:
    return Variable.get(nome, default=padrao)


def _hoje():
    """Dia de coleta = dia real em que a coleta roda (não o intervalo lógico)."""
    return pendulum.now(TZ).date()


def _chegada() -> Path:
    return Path(_var("RUA_ACESA_CHEGADA", "/opt/airflow/chegada"))


def notificar_falha(context) -> None:
    """Notificação simples: registra no log e, se NOTIFY_WEBHOOK_URL existir, avisa por POST."""
    ti = context.get("task_instance")
    msg = f"[gs_coleta] falha na tarefa {getattr(ti, 'task_id', '?')}: {context.get('exception')}"
    log.error(msg)
    url = _var("NOTIFY_WEBHOOK_URL", "")
    if url:
        try:
            requests.post(url, json={"text": msg}, timeout=10)
        except requests.RequestException as e:  # notificação nunca derruba o DAG
            log.warning("Webhook indisponível: %s", e)


with DAG(
    dag_id="gs_coleta",
    description="Coleta diária: SP156, GeoSampa (iluminação e distritos) e Open-Meteo",
    schedule="0 6 * * *",  # todo dia às 06h (America/Sao_Paulo)
    start_date=datetime(2026, 11, 5, tzinfo=TZ),
    end_date=datetime(2026, 11, 7, 23, 59, tzinfo=TZ),  # agenda só até sábado 07/11
    catchup=False,  # só dias reais de coleta, sem gerar arquivos de datas passadas
    max_active_runs=1,
    default_args={
        "owner": "rua-acesa",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notificar_falha,
    },
    tags=["global-solution", "rua-acesa", "coleta"],
) as dag:

    @task
    def coletar_sp156() -> dict:
        servicos = json.loads(_var("SP156_SERVICOS", json.dumps(coleta.SP156_SERVICOS_PADRAO)))
        res = coleta.coletar_sp156(
            _chegada() / "sp156", _hoje(), servicos, int(_var("SP156_N_TRIMESTRES", "2"))
        )
        log.info("SP156: %s", res)
        return res

    @task
    def coletar_iluminacao() -> dict:
        res = coleta.coletar_geosampa_iluminacao(
            _chegada() / "geosampa", _hoje(), int(_var("GEOSAMPA_PAGINA", "20000"))
        )
        log.info("GeoSampa iluminação: %s", res)
        return res

    @task
    def coletar_distritos() -> dict:
        res = coleta.coletar_geosampa_distritos(_chegada() / "geosampa", _hoje())
        log.info("GeoSampa distritos: %s", res)
        return res

    @task
    def coletar_clima() -> dict:
        res = coleta.coletar_openmeteo(
            _chegada() / "openmeteo", _hoje(),
            float(_var("OPENMETEO_LAT", "-23.55")), float(_var("OPENMETEO_LON", "-46.63")),
            int(_var("OPENMETEO_DIAS", "120")),
        )
        log.info("Open-Meteo: %s", res)
        return res

    @task
    def conferir_arquivos(*_resultados) -> dict:
        try:
            resumo = coleta.conferir_arquivos(_chegada(), _hoje())
        except ValueError as e:
            raise AirflowFailException(str(e)) from e
        log.info("Conferência OK: %s", json.dumps(resumo, ensure_ascii=False))
        return resumo

    conferir_arquivos(coletar_sp156(), coletar_iluminacao(), coletar_distritos(), coletar_clima())
