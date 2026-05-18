import sys
import great_expectations as gx

context    = gx.get_context(context_root_dir="gx")
DATASOURCE = "snowflake_hotmart"

TABLES = {
    "purchase":             "suite_purchase",
    "product_item":         "suite_product_item",
    "purchase_extra_info":  "suite_purchase_extra_info",
    "gmv_hist":             "suite_gmv_hist",
}


def run_checkpoint(table: str, suite: str) -> bool:
    name = f"checkpoint_{table}"
    config = {
        "name": name,
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": f"%Y%m%d-%H%M%S-{table}",
        "validations": [{
            "batch_request": {
                "datasource_name": DATASOURCE,
                "data_connector_name": "configured_connector",
                "data_asset_name": table,
            },
            "expectation_suite_name": suite,
        }],
    }
    try:
        context.delete_checkpoint(name)
    except Exception:
        pass
    context.add_checkpoint(**config)
    result = context.run_checkpoint(checkpoint_name=name)
    ok = result["success"]
    print(f"  {'OK' if ok else 'FALHOU'}  {table.upper()}")
    return ok


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else None
    tables = {target: TABLES[target]} if target and target in TABLES else TABLES
    results = {t: run_checkpoint(t, s) for t, s in tables.items()}
    context.build_data_docs()
    # remova esse loop duplicado:
    # for t, ok in results.items():
    #     print(f"  {'OK' if ok else 'FALHOU'}  {t}")
    if not all(results.values()):
        sys.exit(1)
    print("Todas as validacoes passaram!")


if __name__ == "__main__":
    main()
