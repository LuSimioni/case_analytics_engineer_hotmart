"""
gmv_hist_dag.py — Pipeline GMV histórico por subsidiária (D-1)
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

PROJECT_PATH = Path("/opt/airflow/project")
GX_DIR       = PROJECT_PATH / "gx"
SQL_PATH     = PROJECT_PATH / "queries" / "ddl" /"gmv_hist.sql"


# ─── tasks ───────────────────────────────────────────────────────────────────

@task
def validar_fontes():
    import great_expectations as gx
    ctx    = gx.get_context(context_root_dir=str(GX_DIR))
    falhas = [
        t for t in ["purchase", "product_item", "purchase_extra_info"]
        if not ctx.run_checkpoint(checkpoint_name=f"checkpoint_{t}")["success"]
    ]
    if falhas:
        raise ValueError(f"GX falhou: {falhas}")


@task
def rodar_etl(ds: str):
    from snowflake.snowpark import Session
    ref_date = date.fromisoformat(ds) - timedelta(days=1)

    session = Session.builder.configs({
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "database":  os.environ["SNOWFLAKE_DATABASE"],
        "schema":    "EVENTS_HOTMART",
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role":      os.environ["SNOWFLAKE_ROLE"],
    }).create()

    sql = SQL_PATH.read_text().format(ref_date=ref_date)
    for stmt in (s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")):
        session.sql(stmt).collect()

    session.close()


@task
def validar_saida():
    import great_expectations as gx
    ctx    = gx.get_context(context_root_dir=str(GX_DIR))
    result = ctx.run_checkpoint(checkpoint_name="checkpoint_gmv_hist")
    if not result["success"]:
        raise ValueError("GX falhou no gmv_hist.")


# ─── DAG ─────────────────────────────────────────────────────────────────────

@dag(
    dag_id="gmv_hist_pipeline",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "owner":            "analytics_engineer",
        "retries":          2,
        "retry_delay":      timedelta(minutes=5),
        "email_on_failure": False,
    },
    tags=["gmv", "hotmart", "scd2", "snowpark"],
)
def gmv_hist_pipeline():
    validar_fontes() >> rodar_etl() >> validar_saida()


gmv_hist_pipeline()