import argparse
import datetime
import gzip
import json
import os
import random
from typing import Literal, TypedDict

import pandas as pd  # type: ignore
from faker import Faker  # type: ignore
from unidecode import unidecode  # type: ignore

faker: Faker = Faker(locale="pt_BR")


def strip_accents(input_string: str) -> str:
    stripped_string: str = unidecode(input_string)

    return stripped_string


def generate_random_domain_name() -> str:
    common_domains: list[str] = [
        "gmail.com",
        "yahoo.com",
        "hotmail.com",
        "outlook.com",
        "icloud.com",
        "aol.com",
        "mail.com",
        "zoho.com",
        "protonmail.com",
        "yandex.com",
        "gmx.com",
        "me.com",
        "msn.com",
        "live.com",
        "comcast.net",
        "verizon.net",
        "att.net",
        "bellsouth.net",
        "earthlink.net",
        "charter.net",
        "cox.net",
        "rocketmail.com",
        "inbox.com",
        "aim.com",
        "mac.com",
        "fastmail.com",
        "rediffmail.com",
        "hushmail.com",
        "tutanota.com",
        "lycos.com",
        "netzero.net",
        "juno.com",
    ]

    random_domain_name: str = random.choice(common_domains)

    return random_domain_name


def generate_random_email(
    first_name: str, last_name: str, domain_name: str | None = None
) -> str:
    if not domain_name:
        domain_name = generate_random_domain_name()

    username_separator: str = random.choice(["", ".", "-", "_"])

    # The first and last name may contain more than one word, so we take only the first and last part respectively
    first_username: str = strip_accents(first_name).lower().split(" ")[0]
    last_username: str = strip_accents(last_name).lower().split(" ")[-1]

    email: str = f"{first_username}{username_separator}{last_username}@{domain_name}"

    return email


class ClientData(TypedDict):
    id: int
    first_name: str
    last_name: str
    cpf: str
    rg: str
    email: str
    birth_date: str | datetime.date


class Client:
    def __init__(self) -> None:
        self.id: int = faker.random_int(1, int(1e6), 1)
        self.first_name: str = faker.first_name()
        self.last_name: str = faker.last_name()
        self.cpf: str = faker.cpf()
        self.rg: str = faker.rg()
        self.email: str = generate_random_email(self.first_name, self.last_name)
        self.birth_date: str | datetime.date = faker.date_of_birth(
            minimum_age=18, maximum_age=95
        ).strftime("%Y-%m-%d")

    def to_json(self) -> ClientData:
        return {
            "id": self.id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "cpf": self.cpf,
            "rg": self.rg,
            "email": self.email,
            "birth_date": self.birth_date,
        }


class OrderData(TypedDict):
    id: int
    client_id: int
    order_date: str
    order_price: float


class Order:
    def __init__(self, client: Client) -> None:
        self.id: int = faker.random_int(1, int(1e9), 1)
        self.client: Client = client
        self.order_date: str = faker.date_time_between(
            start_date=datetime.datetime(2015, 1, 1), end_date=datetime.datetime.now()
        ).strftime("%Y-%m-%d %H:%M:%S")
        self.order_price: float = faker.pyfloat(
            left_digits=5, right_digits=2, positive=True
        )

    def to_json(self) -> OrderData:
        return {
            "id": self.id,
            "client_id": self.client.id,
            "order_date": self.order_date,
            "order_price": self.order_price,
        }


class OrderItemData(TypedDict):
    id: int
    order_id: int
    item_name: str
    item_price: float
    item_description: str


class OrderItem:
    def __init__(
        self, order: Order, item_name: str, item_price: float, item_description: str
    ) -> None:
        self.id: int = faker.random_int(1, int(1e9), step=1)
        self.order: Order = order
        self.item_name: str = item_name
        self.item_price: float = item_price
        self.item_description: str = item_description

    def to_json(self) -> OrderItemData:
        return {
            "id": self.id,
            "item_name": self.item_name,
            "item_price": self.item_price,
            "item_description": self.item_description,
        }


class OrderItems:
    def __init__(self, order: Order, num_items: int) -> None:
        self.items = self.generate_items(order, num_items)

    def generate_items(self, order: Order, num_items: int) -> list[OrderItem]:
        item_names: list[str] = [faker.word() for _ in range(num_items)]
        descriptions: list[str] = [
            " ".join(faker.words(faker.random_int(1, 6))) for _ in range(num_items)
        ]

        prices: list[float] = self.split_price(order.order_price, num_items)

        return [
            OrderItem(order, item_name, item_price, item_description)
            for item_name, item_price, item_description in zip(
                item_names, prices, descriptions
            )
        ]

    @staticmethod
    def split_price(order_price: float, num_items: int) -> list[float]:
        prices: list[float] = [random.random() for _ in range(num_items)]
        price_sum: float = sum(prices)

        prices = [round((order_price * price) / price_sum, 2) for price in prices]

        prices[-1] = round(order_price - sum(prices[:-1]), 2)

        return prices

    def to_json(self) -> list[OrderItemData]:
        return [item.to_json() for item in self.items]


class PaymentData(TypedDict):
    order_id: int
    client_id: int
    method: str
    amount: float
    installments: int
    tranches: list[dict[str, float | str | int]]


class Payment:
    def __init__(
        self,
        order: Order,
        method: Literal["cash", "credit_card", "debit_card"],
        installments: int,
    ) -> None:
        self.order: Order = order
        self.client_id: int = self.order.client.id
        self.id: int = faker.random_int(1, int(1e9), step=1)
        self.method: str = method.lower()
        self.order_date: datetime.datetime = datetime.datetime.strptime(
            order.order_date, "%Y-%m-%d %H:%M:%S"
        )
        self.amount: float = order.order_price
        self.installments: int = installments if self.method == "credit_card" else 1
        self.tranches: list[dict[str, float | str | int]] = (
            self.generate_tranches() if self.method == "credit_card" else []
        )

    def generate_tranches(self) -> list[dict[str, float | str | int]]:
        if not self.installments or self.installments < 1:
            raise ValueError(
                "Installments must be a positive integer for credit card payments"
            )

        base_trench: float = round(self.amount / self.installments, 2)

        tranches: list[float] = [base_trench] * self.installments
        tranches[-1] = round(self.amount - sum(tranches[:-1]), 2)

        tranche_details: list[dict[str, float | str | int]] = []

        for i in range(self.installments):
            payment_date: str = (
                self.order_date + datetime.timedelta(days=30 * (i + 1))
            ).strftime("%Y-%m-%d")
            tranche_details.append(
                {
                    "value": tranches[i],
                    "date_of_payment": payment_date,
                    "installment_number": i + 1,
                }
            )

        return tranche_details

    def to_json(self) -> PaymentData:
        return {
            "id": self.id,
            "method": self.method,
            "amount": self.amount,
            "installments": self.installments,
            "tranches": self.tranches if self.method == "credit_card" else [],
        }


def generate_order() -> dict[str, str | int | float | bytes]:
    client: Client = Client()
    order: Order = Order(client)

    num_items: int = faker.random_int(1, 100, 1)
    order_items: OrderItems = OrderItems(order, num_items)

    payment_method: Literal["cash", "credit_card", "debit_card"] = random.choice(
        ["cash", "credit_card", "debit_card"]
    )
    installments: int = (
        faker.random_int(1, 48, 1) if payment_method == "credit_card" else 1
    )
    payment: Payment = Payment(order, payment_method, installments)

    order_json: dict[
        str, int | str | float | ClientData | list[OrderItemData] | PaymentData
    ] = {
        "order_id": order.id,
        "order_date": order.order_date,
        "order_price": order.order_price,
        "client": client.to_json(),
        "order_items": order_items.to_json(),
        "payment": payment.to_json(),
    }

    order_json_str: str = json.dumps(order_json, ensure_ascii=False)

    compressed_order_json: bytes = gzip.compress(order_json_str.encode("utf-8"))

    return {
        "order_id": order.id,
        "order_date": order.order_date,
        "order_price": order.order_price,
        "client_id": client.id,
        "compressed_json": compressed_order_json,
    }


def main(num_orders: int) -> pd.DataFrame:
    orders_data = (generate_order() for _ in range(num_orders))

    df = pd.DataFrame(orders_data)

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Generate a specified number of orders and save them as parquet.\n\n"
            "Example usage:\n"
            "    python decompress_json.py --num_orders 100 --output_path ./orders"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--num_orders",
        type=int,
        help="Number of orders to be generated (integer)",
    )
    parser.add_argument(
        "--output_path",
        required=False,
        type=str,
        help="Folder path to save the generated file",
    )

    args = parser.parse_args()
    num_orders = args.num_orders
    output_path = args.output_path

    # Check if the directory of the specified output folder path exists and is writable
    if not os.access(os.path.dirname(output_path), os.W_OK):
        raise ValueError(f"The specified directory is not writable: {output_path}")

    df = main(num_orders)
    df.to_parquet(f"{output_path}/compressed_json.parquet", index=False)
