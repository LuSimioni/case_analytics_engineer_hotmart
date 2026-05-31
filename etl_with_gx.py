import os
import sys
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from snowflake.snowpark import Session
import great_expectations as gx

load_dotenv()

_gx_context = gx.get_context(context_root_dir="gx")
import expect_query_result_to_be_empty  # noqa: E402 — must come after get_context adds plugins/ to sys.path

YESTERDAY = date.today() - timedelta(days=1)
SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA", "events_hotmart").upper()
SOURCES   = ["purchase", "product_item", "purchase_extra_info"]


def load_sql(filename: str, ref_date: date) -> list[str]:
    sql = (Path(__file__).parent / "queries" / filename).read_text(encoding="utf-8").format(ref_date=ref_date)
    return [
        "\n".join(l for l in block.splitlines() if l.strip() and not l.strip().startswith("--"))
        for block in sql.split(";")
        if any(l.strip() and not l.strip().startswith("--") for l in block.splitlines())
    ]


@contextmanager
def snowflake_session():
    session = Session.builder.configs({
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "database":  os.environ["SNOWFLAKE_DATABASE"],
        "schema":    SCHEMA,
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role":      os.environ["SNOWFLAKE_ROLE"],
    }).create()
    try:
        yield session
    finally:
        session.close()


def merge_scd2(session: Session, ref_date: date) -> None:
    for statement in load_sql("ddl/gmv_hist.sql", ref_date):
        session.sql(statement).collect()


def validate(table: str) -> bool:
    result = _gx_context.run_checkpoint(checkpoint_name=f"checkpoint_{table}")
    if not result["success"]:
        print(f"[GX] FALHOU: {table.upper()}")
        for _, val_result in result.run_results.items():
            for exp_result in val_result["validation_result"].results:
                if not exp_result.success:
                    cfg = exp_result.expectation_config
                    print(f"  ✗ {cfg.expectation_type}")
                    if "query" in cfg.kwargs:
                        print(f"    query: {cfg.kwargs['query'].strip()}")
                    rows = exp_result.result.get("unexpected_rows") or []
                    if rows:
                        print(f"    linhas inesperadas ({len(rows)}):")
                        for r in rows:
                            print(f"      {r}")
    return result["success"]


def validate_all(tables: list[str]) -> bool:
    return all(validate(t) for t in tables)


def main(ref_date: date = YESTERDAY) -> None:
    print(f"[GMV] Pipeline {ref_date}")

    if not validate_all(SOURCES):
        sys.exit("[GMV] Fontes inválidas. Abortando.")

    with snowflake_session() as session:
        merge_scd2(session, ref_date)

        if not validate("gmv_hist"):
            sys.exit("[GMV] gmv_hist inválido.")

    _gx_context.build_data_docs()
    print("[GMV] Concluído — gx/uncommitted/data_docs/local_site/index.html")


if __name__ == "__main__":
    main(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else YESTERDAY)