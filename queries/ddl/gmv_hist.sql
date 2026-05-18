MERGE INTO EVENTS_HOTMART.gmv_hist AS target
USING (
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
    ),

    source_data AS (
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
            pu.transaction_datetime,
            HASH(
                pu.buyer_id, pu.producer_id, pi.product_id,
                ex.subsidiary, pu.purchase_status,
                pu.order_date, pu.release_date,
                pu.purchase_total_value, pi.item_quantity, pi.purchase_value
            ) AS row_hash
        FROM purchase_snapshot pu
        LEFT JOIN product_item_snapshot pi
            ON  pi.prod_item_id        = pu.prod_item_id
            AND pi.prod_item_partition = pu.prod_item_partition
        LEFT JOIN extra_info_snapshot ex
            ON  ex.purchase_id        = pu.purchase_id
            AND ex.purchase_partition = pu.purchase_partition
    ),

    classified AS (
        SELECT
            s.*,
            CASE
                WHEN h.purchase_id IS NULL       THEN 'NEW'
                WHEN h.row_hash != s.row_hash    THEN 'CHANGED'
                ELSE 'UNCHANGED'
            END AS change_type
        FROM source_data s
        LEFT JOIN EVENTS_HOTMART.gmv_hist h
            ON  h.purchase_id = s.purchase_id
            AND '{ref_date}' >= h.valid_from
            AND '{ref_date}' < COALESCE(h.valid_to, '9999-12-31')
    ),

    merge_source AS (
        SELECT c.*, 'CLOSE' AS merge_action
        FROM classified c
        WHERE c.change_type = 'CHANGED'

        UNION ALL

        SELECT c.*, 'OPEN' AS merge_action
        FROM classified c
        WHERE c.change_type IN ('NEW', 'CHANGED')
    )

    SELECT * FROM merge_source
) AS source
ON  target.purchase_id  = source.purchase_id
AND target.is_current   = TRUE
AND source.merge_action = 'CLOSE'

WHEN MATCHED THEN UPDATE SET
    target.valid_to   = '{ref_date}',
    target.is_current = FALSE

WHEN NOT MATCHED AND source.merge_action = 'OPEN' THEN INSERT (
    purchase_id, purchase_partition,
    buyer_id, producer_id, product_id,
    subsidiary, purchase_status,
    order_date, release_date,
    purchase_total_value, item_quantity, purchase_value,
    row_hash,
    valid_from, valid_to, is_current,
    transaction_date, transaction_datetime
) VALUES (
    source.purchase_id, source.purchase_partition,
    source.buyer_id, source.producer_id, source.product_id,
    source.subsidiary, source.purchase_status,
    source.order_date, source.release_date,
    source.purchase_total_value, source.item_quantity, source.purchase_value,
    source.row_hash,
    '{ref_date}', NULL, TRUE,
    source.transaction_date, source.transaction_datetime
)
;