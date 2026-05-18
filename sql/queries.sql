--Quais são os 50 maiores produtores em faturamento ($) de 2021?

SELECT
    producer_id,
    SUM(purchase_total_value) AS total_value
FROM ANALYTICS.HOTMART.PURCHASE
WHERE order_date >= '2021-01-01'      
  AND order_date <  '2022-01-01'     
  AND release_date IS NOT NULL 
GROUP BY producer_id
ORDER BY total_value DESC             
LIMIT 50;

select count(distinct(product_id))
from analytics.hotmart.product_item




--Quais são os 2 produtos que mais faturaram ($) de cada produtor?

WITH product_revenue AS (
    SELECT
        pu.producer_id,
        pr.product_id,
        SUM(pu.purchase_total_value) AS total_revenue
    FROM ANALYTICS.HOTMART.PRODUCT_ITEM pr
    INNER JOIN ANALYTICS.HOTMART.PURCHASE pu
        ON pr.prod_item_id = pu.prod_item_id
       AND pr.prod_item_partition = pu.prod_item_partition
    WHERE release_date IS NOT NULL
    GROUP BY
        pu.producer_id,
        pr.product_id
),

ranked_products AS (
    SELECT
        producer_id,
        product_id,
        total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY producer_id
            ORDER BY total_revenue DESC
        ) AS rn
    FROM product_revenue
)

SELECT
    producer_id,
    product_id,
    total_revenue
FROM ranked_products
WHERE rn <= 2
ORDER BY producer_id, total_revenue DESC;