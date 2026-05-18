import os
import sys
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from snowflake.snowpark import Session
import great_expectations as gx

load_dotenv()


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
    result = gx.get_context(context_root_dir="gx").run_checkpoint(checkpoint_name=f"checkpoint_{table}")
    if not result["success"]:
        print(f"[GX] FALHOU: {table.upper()}")
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

    gx.get_context(context_root_dir="gx").build_data_docs()
    print("[GMV] Concluído — gx/uncommitted/data_docs/local_site/index.html")


if __name__ == "__main__":
    main(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else YESTERDAY)