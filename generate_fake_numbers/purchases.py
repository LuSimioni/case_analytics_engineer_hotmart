import random
from datetime import timedelta

import pandas as pd

from generate_fake_numbers.factorys import PurchaseFactory
from generate_fake_numbers.product_items import generate_product_items_csv


STATUS_FLOWS = {
    "INICIADA":    ["INICIADA"],
    "APROVADA":    ["INICIADA", "APROVADA"],
    "CANCELADA":   ["INICIADA", "CANCELADA"],
    "REEMBOLSADA": ["INICIADA", "APROVADA", "REEMBOLSADA"],
}


def expand_to_status_rows(purchase):
    flow = STATUS_FLOWS[purchase.purchase_status]
    rows = []
    dt = purchase.transaction_datetime

    for i, status in enumerate(flow):
        if i > 0:
            dt = dt + timedelta(hours=random.randint(12, 72))

        # INICIADA ainda não foi liberada; CANCELADA nunca tem release_date
        release_date = None if status in ("INICIADA", "CANCELADA") else purchase.release_date

        rows.append({
            "purchase_id":          purchase.purchase_id,
            "purchase_partition":   purchase.purchase_partition,
            "buyer_id":             purchase.buyer_id,
            "prod_item_id":         purchase.prod_item_id,
            "prod_item_partition":  purchase.prod_item_partition,
            "order_date":           purchase.order_date,
            "release_date":         release_date,
            "producer_id":          purchase.producer_id,
            "purchase_total_value": purchase.purchase_total_value,
            "purchase_status":      status,
            "transaction_datetime": dt,
            "transaction_date":     dt.date(),
        })

    return rows


def generate_purchases_csv(
    quantity: int = 100,
    num_product_items: int = 100,
    num_producers: int = 20,
    output_file: str = "purchases.csv",
):
    product_items = generate_product_items_csv(quantity=num_product_items)

    # Agrupar itens por product_id para garantir diversidade por produtor
    product_id_to_items: dict = {}
    for item in product_items:
        product_id_to_items.setdefault(item.product_id, []).append(item)
    all_product_ids = list(product_id_to_items.keys())

    # Cada produtor recebe 3-5 product_ids distintos
    producers = list(range(1, num_producers + 1))
    producer_items = {}
    for p in producers:
        n = random.randint(3, min(5, len(all_product_ids)))
        assigned_pids = random.sample(all_product_ids, k=n)
        producer_items[p] = [random.choice(product_id_to_items[pid]) for pid in assigned_pids]

    # Gerar compras: cada uma referencia um item do pool do produtor
    purchases = []
    for _ in range(quantity):
        producer_id = random.choice(producers)
        product_item = random.choice(producer_items[producer_id])
        purchases.append(PurchaseFactory.build(
            product_item=product_item,
            producer_id=producer_id,
        ))

    # Expandir cada compra em histórico de status
    all_rows = []
    for p in purchases:
        all_rows.extend(expand_to_status_rows(p))

    df = pd.DataFrame(all_rows)
    df.to_csv(output_file, index=False)
    print(f"CSV gerado: {output_file} — {len(all_rows)} linhas ({quantity} compras)")


if __name__ == "__main__":
    generate_purchases_csv(quantity=1000, num_product_items=100, num_producers=100)
