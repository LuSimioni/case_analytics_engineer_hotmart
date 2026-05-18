/*
==============================================================================
  Statement: MERGE SCD2 da gmv_hist
  Frequência: diária (D-1)
  Statement único, atômico, sem temp tables.
==============================================================================

  ESTRATÉGIA
  ----------
  1) Para cada uma das 3 fontes (purchase / product_item / purchase_extra_info),
     consolido o "último evento por entidade" até a data de referência.
     -- Atende: granularidade diária + "considerar o último evento válido do dia"

  2) Faço LEFT JOIN entre as 3 fontes pela chave da compra.
     -- Atende: "se uma tabela atualizou e as demais não, repete os ativos
        das demais" — porque o snapshot pega o último estado conhecido até
        ref_date, independentemente de ter mudado nesse dia ou não.

  3) Calculo um HASH dos atributos de negócio (row_hash).
     -- Substitui comparação coluna a coluna por igualdade de NUMBER.

  4) Comparo com a versão corrente da gmv_hist (is_current = TRUE):
     - NEW       → não existe versão atual          → INSERT
     - CHANGED   → existe e o hash mudou            → UPDATE (fecha) + INSERT (abre)
     - UNCHANGED → existe e o hash é igual          → descarta (não paga custo)

  5) Para resolver "fechar + abrir" num único MERGE, uso o truque do dummy join:
     duplico via UNION ALL cada linha CHANGED em duas ações:
       - CLOSE → casa com is_current=TRUE  → UPDATE
       - OPEN  → não casa                  → INSERT
     Linhas NEW entram só com OPEN. Linhas UNCHANGED não entram.

  IDEMPOTÊNCIA
  ------------
  Rodar duas vezes para o mesmo ref_date é seguro:
  - Na 2ª execução, o hash já está igual → UNCHANGED → nada acontece.
==============================================================================
*/

MERGE INTO EVENTS_HOTMART.gmv_hist AS target
USING (
    WITH
    /* ===== 1. Snapshots: último evento por entidade até ref_date ===== */
    purchase_snapshot AS (
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

    /* ===== 2. Visão consolidada da compra (LEFT JOIN pela purchase) ===== */
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

            HASH(
                pu.buyer_id, pu.producer_id, pi.product_id,
                ex.subsidiary, pu.purchase_status,
                pu.order_date, pu.release_date,
                pu.purchase_total_value, pi.item_quantity, pi.purchase_value
            ) AS row_hash,

            GREATEST(
                pu.transaction_datetime,
                COALESCE(pi.transaction_datetime, pu.transaction_datetime),
                COALESCE(ex.transaction_datetime, pu.transaction_datetime)
            ) AS last_source_event_datetime

        FROM purchase_snapshot pu
        LEFT JOIN product_item_snapshot pi
            ON  pi.prod_item_id        = pu.prod_item_id
            AND pi.prod_item_partition = pu.prod_item_partition
        LEFT JOIN extra_info_snapshot ex
            ON  ex.purchase_id        = pu.purchase_id
            AND ex.purchase_partition = pu.purchase_partition
    ),

    /* ===== 3. Classifica: NEW / CHANGED / UNCHANGED ===== */
    classified AS (
        SELECT
            s.*,
            CASE
                WHEN h.purchase_id IS NULL     THEN 'NEW'
                WHEN h.row_hash != s.row_hash  THEN 'CHANGED'
                ELSE 'UNCHANGED'
            END AS change_type
        FROM source_data s
        LEFT JOIN EVENTS_HOTMART.gmv_hist h
            ON  h.purchase_id = s.purchase_id
            AND h.is_current  = TRUE
    ),

    /* ===== 4. Dummy join: duplica CHANGED em CLOSE + OPEN ===== */
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

/* ===== 5. Fecha a versão antiga ===== */
WHEN MATCHED THEN UPDATE SET
    target.valid_to   = '{ref_date}',
    target.is_current = FALSE

/* ===== 6. Abre a nova versão ===== */
WHEN NOT MATCHED AND source.merge_action = 'OPEN' THEN INSERT (
    purchase_id, purchase_partition,
    buyer_id, producer_id, product_id,
    subsidiary, purchase_status,
    order_date, release_date,
    purchase_total_value, item_quantity, purchase_value,
    row_hash,
    valid_from, valid_to, is_current,
    transaction_date, last_source_event_datetime
) VALUES (
    source.purchase_id, source.purchase_partition,
    source.buyer_id, source.producer_id, source.product_id,
    source.subsidiary, source.purchase_status,
    source.order_date, source.release_date,
    source.purchase_total_value, source.item_quantity, source.purchase_value,
    source.row_hash,
    '{ref_date}', NULL, TRUE,
    '{ref_date}', source.last_source_event_datetime
)
;