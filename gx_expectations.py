import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from datetime import date
from dotenv import load_dotenv

load_dotenv()

context = gx.get_context(context_root_dir="gx")
import expect_query_result_to_be_empty  
DATASOURCE = "snowflake_hotmart"
CONNECTOR  = "configured_connector"


def get_validator(table: str, suite_name: str):
    return context.get_validator(
        batch_request=BatchRequest(
            datasource_name=DATASOURCE,
            data_connector_name=CONNECTOR,
            data_asset_name=table,
        ),
        expectation_suite_name=suite_name,
    )


def save(v):
    v.save_expectation_suite(discard_failed_expectations=False)


# ── PURCHASE ────────────────────────────────────────────────────────────────
v = get_validator("purchase", "suite_purchase")

v.expect_table_row_count_to_be_between(min_value=1)

for col in ("purchase_id", "transaction_date", "transaction_datetime", "order_date"):
    v.expect_column_values_to_not_be_null(col)

v.expect_column_values_to_be_between("purchase_id",          min_value=1)
v.expect_column_values_to_be_between("buyer_id",             min_value=1, mostly=0.99)
v.expect_column_values_to_be_between("producer_id",          min_value=1, mostly=0.99)
v.expect_column_values_to_be_between("prod_item_id",         min_value=1, mostly=0.99)
v.expect_column_values_to_be_between("purchase_partition",   min_value=0, mostly=0.99)
v.expect_column_values_to_be_between("purchase_total_value", min_value=0)

v.expect_column_values_to_be_in_set(
    "purchase_status",
    ["INICIADA", "APROVADA", "CANCELADA", "REEMBOLSADA"],
    mostly=0.95,
)

save(v)


v.expect_table_row_count_to_be_between(min_value=1)

# obrigatórios
for col in ("purchase_id", "transaction_date", "transaction_datetime", "order_date"):
    v.expect_column_values_to_not_be_null(col)


# ── PRODUCT_ITEM ─────────────────────────────────────────────────────────────
v = get_validator("product_item", "suite_product_item")

v.expect_table_row_count_to_be_between(min_value=1)

for col in ("prod_item_id", "transaction_date", "transaction_datetime"):
    v.expect_column_values_to_not_be_null(col)

v.expect_column_values_to_be_between("prod_item_id",        min_value=1)
v.expect_column_values_to_be_between("prod_item_partition", min_value=0)
v.expect_column_values_to_be_between("product_id",         min_value=1, mostly=0.95)
v.expect_column_values_to_be_between("item_quantity", min_value=1)
v.expect_column_values_to_be_between("purchase_value",     min_value=0)

save(v)


# ── PURCHASE_EXTRA_INFO ──────────────────────────────────────────────────────
v = get_validator("purchase_extra_info", "suite_purchase_extra_info")

v.expect_table_row_count_to_be_between(min_value=1)

for col in ("purchase_id", "transaction_date", "transaction_datetime"):
    v.expect_column_values_to_not_be_null(col)

v.expect_column_values_to_be_between("purchase_id",       min_value=1)
v.expect_column_values_to_be_between("purchase_partition", min_value=0)

v.expect_column_values_to_not_be_null("subsidiary", mostly=0.90)
v.expect_column_values_to_be_in_set(
    "subsidiary",
    ["nacional", "internacional"],
    mostly=0.90,  # alinha com o not_be_null
)

save(v)


# ── GMV_HIST ─────────────────────────────────────────────────────────────────
v = get_validator("gmv_hist", "suite_gmv_hist")

for col in ("purchase_id", "valid_from", "is_current", "transaction_date"):
    v.expect_column_values_to_not_be_null(col)

v.expect_column_values_to_be_between("purchase_id",          min_value=1)
v.expect_column_values_to_be_between("purchase_total_value", min_value=0)

v.expect_column_values_to_be_in_set("is_current",  [True, False])
v.expect_column_values_to_be_in_set("subsidiary",  ["nacional", "internacional"], mostly=0.85)
v.expect_column_values_to_be_in_set(
    "purchase_status",
    ["INICIADA", "APROVADA", "CANCELADA", "REEMBOLSADA"],
    mostly=0.99,
)
v.expect_column_values_to_be_between(
    "valid_from",
    min_value="2020-01-01",
    max_value=str(date.today()),
    parse_strings_as_datetimes=True,
)
v.expect_column_values_to_not_be_null("product_id", mostly=0.99)

v.expect_query_result_to_be_empty(query="""
SELECT purchase_id
FROM EVENTS_HOTMART.gmv_hist
GROUP BY purchase_id
HAVING COUNT_IF(is_current = TRUE) <> 1
""")

v.expect_query_result_to_be_empty(query="""
SELECT *
FROM EVENTS_HOTMART.gmv_hist
WHERE valid_from > valid_to
  AND valid_from IS NOT NULL
  AND valid_to IS NOT NULL
""")

v.expect_query_result_to_be_empty(query="""
SELECT *
FROM EVENTS_HOTMART.gmv_hist
WHERE is_current = TRUE
AND valid_to IS NOT NULL
""")

v.expect_query_result_to_be_empty(query="""
SELECT *
FROM EVENTS_HOTMART.gmv_hist
WHERE is_current = FALSE
AND valid_to IS NULL
""")

v.expect_query_result_to_be_empty(query="""
SELECT a.purchase_id
FROM EVENTS_HOTMART.gmv_hist a
JOIN EVENTS_HOTMART.gmv_hist b
    ON a.purchase_id = b.purchase_id
   AND a.valid_from < COALESCE(b.valid_to, '9999-12-31')
   AND COALESCE(a.valid_to, '9999-12-31') > b.valid_from
   AND a.valid_from <> b.valid_from
""")

v.expect_query_result_to_be_empty(query="""
SELECT purchase_id
FROM EVENTS_HOTMART.gmv_hist
GROUP BY purchase_id
HAVING MAX(valid_from) != MAX(CASE WHEN is_current THEN valid_from END)
""")

v.expect_query_result_to_be_empty(query="""
SELECT purchase_id, valid_from
FROM EVENTS_HOTMART.gmv_hist
GROUP BY purchase_id, valid_from
HAVING COUNT(*) > 1
""")


save(v)