import csv
from pathlib import Path

from generate_fake_numbers.factorys import ProductItemFactory


def generate_product_items_csv(
    quantity: int = 100,
    output_file: str = "product_items.csv",
):
    """
    Gera um batch de ProductItem e salva em CSV.
    """

    product_items = ProductItemFactory.build_batch(quantity)

    output_path = Path(output_file)

    with open(output_path, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        # Header
        writer.writerow([
            "prod_item_id",
            "prod_item_partition",
            "product_id",
            "item_quantity",
            "purchase_value",
            "transaction_datetime",
            "transaction_date",
        ])

        # Rows
        for item in product_items:
            writer.writerow([
                item.prod_item_id,
                item.prod_item_partition,
                item.product_id,
                item.item_quantity,
                item.purchase_value,
                item.transaction_datetime.isoformat(),
                item.transaction_date.isoformat(),
            ])

    print(f"CSV gerado com sucesso em: {output_path.resolve()}")
    return product_items


if __name__ == "__main__":
    generate_product_items_csv(quantity=1000)