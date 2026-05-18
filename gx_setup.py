import os
from pathlib import Path
from dotenv import load_dotenv
import great_expectations as gx

load_dotenv(Path(__file__).parent / ".env")

DATASOURCE = "snowflake_hotmart"
SCHEMA     = os.environ.get("SNOWFLAKE_SCHEMA", "EVENTS_HOTMART").upper()
TABLES     = ["purchase", "product_item", "purchase_extra_info", "gmv_hist"]


def snowflake_conn_str() -> str:
    u = os.environ
    return (
        f"snowflake://{u['SNOWFLAKE_USER']}:{u['SNOWFLAKE_PASSWORD']}"
        f"@{u['SNOWFLAKE_ACCOUNT']}/{u['SNOWFLAKE_DATABASE']}/{SCHEMA}"
        f"?warehouse={u['SNOWFLAKE_WAREHOUSE']}&role={u['SNOWFLAKE_ROLE']}"
    )


def datasource_config() -> dict:
    return {
        "name": DATASOURCE,
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": snowflake_conn_str(),
        },
        "data_connectors": {
            "configured_connector": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "assets": {
                    table: {"schema_name": SCHEMA, "table_name": table.upper()}
                    for table in TABLES
                },
            },
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["run_id"],
            },
        },
    }


def setup(context: gx.DataContext) -> None:
    existing = {ds["name"] for ds in context.list_datasources()}
    if DATASOURCE not in existing:
        context.add_datasource(**datasource_config())

    existing_suites = {s.expectation_suite_name for s in context.list_expectation_suites()}
    for table in TABLES:
        suite = f"suite_{table}"
        if suite not in existing_suites:
            context.add_expectation_suite(suite)


if __name__ == "__main__":
    setup(gx.get_context(context_root_dir="gx"))
    print("[GX] Setup concluído — rode: python gx_expectations.py")