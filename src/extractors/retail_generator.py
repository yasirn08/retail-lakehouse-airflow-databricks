import random
from datetime import UTC, datetime, timedelta
from uuid import uuid4

CITIES = [
    "Islamabad",
    "Lahore",
    "Karachi",
    "Peshawar",
    "Quetta",
    "Faisalabad",
]
CATEGORIES = [
    "Electronics",
    "Home",
    "Clothing",
    "Sports",
    "Books",
]


def generate_customers(count: int = 100) -> list[dict]:
    customers = []

    for customer_id in range(1, count + 1):
        customers.append(
            {
                "customer_id": customer_id,
                "customer_name": f"Customer {customer_id}",
                "email": f"customer{customer_id}@example.com",
                "city": random.choice(CITIES),
                "created_at": datetime.now(UTC).isoformat(),
            }
        )

    return customers


def generate_products(count: int = 50) -> list[dict]:
    products = []

    for product_id in range(1, count + 1):
        products.append(
            {
                "product_id": product_id,
                "product_name": f"Product {product_id}",
                "category": random.choice(CATEGORIES),
                "price": round(random.uniform(5, 1000), 2),
                "created_at": datetime.now(UTC).isoformat(),
            }
        )

    return products


def generate_orders(
    customer_count: int = 100,
    product_count: int = 50,
    order_count: int = 500,
) -> tuple[list[dict], list[dict]]:
    orders = []
    order_items = []

    now = datetime.now(UTC)

    for order_number in range(1, order_count + 1):
        order_id = str(uuid4())

        order_date = now - timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        customer_id = random.randint(1, customer_count)

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date.isoformat(),
                "status": random.choice(
                    [
                        "completed",
                        "completed",
                        "completed",
                        "pending",
                        "cancelled",
                    ]
                ),
            }
        )

        item_count = random.randint(1, 5)

        for item_number in range(1, item_count + 1):
            product_id = random.randint(1, product_count)

            order_items.append(
                {
                    "order_item_id": f"{order_id}-{item_number}",
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": random.randint(1, 5),
                }
            )

    return orders, order_items;