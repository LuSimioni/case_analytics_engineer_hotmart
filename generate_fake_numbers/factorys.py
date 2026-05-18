import factory
from factory import Faker, LazyAttribute, Sequence, SubFactory
from datetime import datetime, date, timedelta
import random

from generate_fake_numbers.fake_generate import (
     ProductItem,
     Purchase,
     PurchaseExtraInfo,
     VALID_PURCHASE_STATUS
 )


# ============================================================
# ProductItemFactory
# ============================================================
class ProductItemFactory(factory.Factory):
    class Meta:
        model = ProductItem

    prod_item_id = Sequence(lambda n: n + 1)
    prod_item_partition = Faker("random_int", min=0, max=10)
    product_id = Faker("random_int", min=1, max=20)
    item_quantity = Faker("random_int", min=1, max=10)
    purchase_value = Faker(
        "pyfloat",
        positive=True,
        min_value=1,
        max_value=2500,
        right_digits=2,
    )

    transaction_datetime = Faker("date_time_between", start_date=datetime(2020, 1, 1), end_date=datetime(2023, 1, 1))

    transaction_date = LazyAttribute(
        lambda obj: obj.transaction_datetime.date()
    )


# ============================================================
# PurchaseFactory
# ============================================================


VALID_PURCHASE_STATUS = [
    "INICIADA",
    "APROVADA",
    "CANCELADA",
    "REEMBOLSADA",
]


class PurchaseFactory(factory.Factory):
    class Meta:
        model = Purchase
        exclude = ["product_item"]

    purchase_id = Sequence(lambda n: n + 1)

    purchase_partition = Faker(
        "random_int",
        min=0,
        max=10,
    )

    buyer_id = Faker(
        "random_int",
        min=1,
        max=999999,
    )

    product_item = SubFactory(ProductItemFactory)

    prod_item_id = LazyAttribute(
        lambda obj: obj.product_item.prod_item_id
    )

    prod_item_partition = LazyAttribute(
        lambda obj: obj.product_item.prod_item_partition
    )

    producer_id = Faker(
        "random_int",
        min=1,
        max=100000,
    )

    purchase_total_value = LazyAttribute(
        lambda obj: round(obj.product_item.purchase_value * obj.product_item.item_quantity, 2)
    )

    order_date = Faker(
        "date_between",
        start_date=date(2021, 1, 1),
        end_date=date(2021, 12, 13),
    )

    purchase_status = Faker(
        "random_element",
        elements=VALID_PURCHASE_STATUS,
    )

    @factory.lazy_attribute
    def release_date(self):

        # cancelada não possui release_date; reembolsada passou por aprovada, então tem
        if self.purchase_status == "CANCELADA":
            return None

        days_delta = random.choices(
            population=range(6),
            weights=[80, 10, 5, 2, 2, 1],
            k=1,
        )[0]
        return self.order_date + timedelta(days=days_delta)

    transaction_datetime = LazyAttribute(
        lambda obj: datetime(
            obj.order_date.year,
            obj.order_date.month,
            obj.order_date.day,
            random.randint(0, 23),
            random.randint(0, 59),
        )
    )

    transaction_date = LazyAttribute(
        lambda obj: obj.transaction_datetime.date()
    )

# ============================================================
# PurchaseExtraInfoFactory
# ============================================================
class PurchaseExtraInfoFactory(factory.Factory):
    class Meta:
        model = PurchaseExtraInfo
        exclude = ["purchase"]

    purchase = SubFactory(PurchaseFactory)

    purchase_id = LazyAttribute(
        lambda obj: obj.purchase.purchase_id
    )

    purchase_partition = LazyAttribute(
        lambda obj: obj.purchase.purchase_partition
    )

    subsidiary = Faker("company")

    transaction_datetime = Faker("date_time_this_year")

    transaction_date = LazyAttribute(
        lambda obj: obj.transaction_datetime.date()
    )