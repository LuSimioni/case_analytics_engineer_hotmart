USE SCHEMA ANALYTICS.EVENTS_HOTMART;
CREATE OR REPLACE TEMPORARY TABLE gmv_trusted_temp AS

WITH purchase_snapshot AS (
    SELECT *
    FROM EVENTS_HOTMART.purchase
    WHERE transaction_date <= '{ref_date}'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY purchase_id
        ORDER BY transaction_datetime DESC
    ) = 1
),

product_item_snapshot AS (
    SELECT *
    FROM EVENTS_HOTMART.product_item
    WHERE transaction_date <= '{ref_date}'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY prod_item_id, prod_item_partition
        ORDER BY transaction_datetime DESC
    ) = 1
),

extra_info_snapshot AS (
    SELECT *
    FROM EVENTS_HOTMART.purchase_extra_info
    WHERE transaction_date <= '{ref_date}'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY purchase_id, purchase_partition
        ORDER BY transaction_datetime DESC
    ) = 1
)

SELECT
    pu.purchase_id,
    pu.purchase_partition,
    pu.buyer_id,
    pu.producer_id,
    pi.product_id,
    ex.subsidiary,
    pu.purchase_status,
    pu.order_date,
    pu.release_date,
    pu.purchase_total_value,
    pi.item_quantity,
    pi.purchase_value,
    pu.transaction_date,
    pu.transaction_datetime
FROM purchase_snapshot pu
LEFT JOIN product_item_snapshot pi
    ON  pi.prod_item_id        = pu.prod_item_id
    AND pi.prod_item_partition = pu.prod_item_partition
LEFT JOIN extra_info_snapshot ex
    ON  ex.purchase_id        = pu.purchase_id
    AND ex.purchase_partition = pu.purchase_partition
;


CREATE OR REPLACE TEMPORARY TABLE gmv_current_snapshot AS
    SELECT purchase_id
    FROM EVENTS_HOTMART.gmv_hist
    WHERE is_current = TRUE
;


MERGE INTO EVENTS_HOTMART.gmv_hist AS target
USING gmv_trusted_temp             AS source
    ON  target.purchase_id = source.purchase_id
    AND target.is_current  = TRUE

WHEN MATCHED AND (
       IFNULL(CAST(target.buyer_id             AS STRING), '') != IFNULL(CAST(source.buyer_id             AS STRING), '')
    OR IFNULL(CAST(target.producer_id          AS STRING), '') != IFNULL(CAST(source.producer_id          AS STRING), '')
    OR IFNULL(CAST(target.product_id           AS STRING), '') != IFNULL(CAST(source.product_id           AS STRING), '')
    OR IFNULL(target.subsidiary,                           '') != IFNULL(source.subsidiary,                           '')
    OR IFNULL(target.purchase_status,                      '') != IFNULL(source.purchase_status,                      '')
    OR IFNULL(CAST(target.order_date           AS STRING), '') != IFNULL(CAST(source.order_date           AS STRING), '')
    OR IFNULL(CAST(target.release_date         AS STRING), '') != IFNULL(CAST(source.release_date         AS STRING), '')
    OR IFNULL(CAST(target.purchase_total_value AS STRING), '') != IFNULL(CAST(source.purchase_total_value AS STRING), '')
    OR IFNULL(CAST(target.item_quantity        AS STRING), '') != IFNULL(CAST(source.item_quantity        AS STRING), '')
    OR IFNULL(CAST(target.purchase_value       AS STRING), '') != IFNULL(CAST(source.purchase_value       AS STRING), '')
) THEN UPDATE SET
    target.valid_to   = '{ref_date}',
    target.is_current = FALSE

WHEN NOT MATCHED THEN INSERT (
    purchase_id, purchase_partition,
    buyer_id, producer_id, product_id,
    subsidiary, purchase_status,
    order_date, release_date,
    purchase_total_value, item_quantity, purchase_value,
    valid_from, valid_to, is_current,
    transaction_date, transaction_datetime
) VALUES (
    source.purchase_id, source.purchase_partition,
    source.buyer_id, source.producer_id, source.product_id,
    source.subsidiary, source.purchase_status,
    source.order_date, source.release_date,
    source.purchase_total_value, source.item_quantity, source.purchase_value,
    '{ref_date}', NULL, TRUE,
    source.transaction_date, source.transaction_datetime
)
;


INSERT INTO EVENTS_HOTMART.gmv_hist (
    purchase_id, purchase_partition,
    buyer_id, producer_id, product_id,
    subsidiary, purchase_status,
    order_date, release_date,
    purchase_total_value, item_quantity, purchase_value,
    valid_from, valid_to, is_current,
    transaction_date, transaction_datetime
)
SELECT
    s.purchase_id, s.purchase_partition,
    s.buyer_id, s.producer_id, s.product_id,
    s.subsidiary, s.purchase_status,
    s.order_date, s.release_date,
    s.purchase_total_value, s.item_quantity, s.purchase_value,
    '{ref_date}', NULL, TRUE,
    s.transaction_date, s.transaction_datetime
FROM gmv_trusted_temp s
INNER JOIN gmv_current_snapshot     pre    ON pre.purchase_id    = s.purchase_id
INNER JOIN EVENTS_HOTMART.gmv_hist  closed ON closed.purchase_id = s.purchase_id
                                          AND closed.valid_to    = '{ref_date}'
                                          AND closed.is_current  = FALSE
WHERE NOT EXISTS (
    SELECT 1
    FROM EVENTS_HOTMART.gmv_hist existing
    WHERE existing.purchase_id = s.purchase_id
      AND existing.valid_from  = '{ref_date}'
      AND existing.is_current  = TRUE
)
;