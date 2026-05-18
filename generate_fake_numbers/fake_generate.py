from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


# ============================================================
# product_item
# ============================================================
@dataclass
class ProductItem:
    prod_item_id: int
    prod_item_partition: int
    product_id: int
    item_quantity: int
    purchase_value: float
    transaction_datetime: datetime
    transaction_date: date


# ============================================================
# purchase
# ============================================================
VALID_PURCHASE_STATUS = {"INICIADA", "APROVADA", "CANCELADA", "REEMBOLSADA"}


@dataclass
class Purchase:
    purchase_id: int
    purchase_partition: int
    prod_item_partition: int
    prod_item_id: int
    buyer_id: int
    order_date: date
    producer_id: int
    purchase_total_value: float
    purchase_status: str
    transaction_datetime: datetime
    transaction_date: date
    release_date: Optional[date] = None


# ============================================================
# purchase_extra_info
# ============================================================
@dataclass
class PurchaseExtraInfo:
    purchase_id: int            # FK -> Purchase.purchase_id
    purchase_partition: int     # FK -> Purchase.purchase_partition
    subsidiary: str
    transaction_datetime: datetime
    transaction_date: date