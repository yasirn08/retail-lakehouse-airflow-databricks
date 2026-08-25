from src.extractors.retail_generator import (
    generate_customers,
    generate_orders,
    generate_products,
)


def test_generate_customers_count():
    customers = generate_customers(10)

    assert len(customers) == 10


def test_customer_ids_are_unique():
    customers = generate_customers(100)

    customer_ids = [
        customer["customer_id"]
        for customer in customers
    ]

    assert len(customer_ids) == len(
        set(customer_ids)
    )


def test_customer_emails_exist():
    customers = generate_customers(20)

    assert all(
        customer["email"]
        for customer in customers
    )


def test_generate_products_count():
    products = generate_products(20)

    assert len(products) == 20


def test_product_ids_are_unique():
    products = generate_products(100)

    product_ids = [
        product["product_id"]
        for product in products
    ]

    assert len(product_ids) == len(
        set(product_ids)
    )


def test_product_prices_are_positive():
    products = generate_products(100)

    assert all(
        product["price"] > 0
        for product in products
    )


def test_generate_orders_count():
    orders, _ = generate_orders(
        customer_count=10,
        product_count=5,
        order_count=50,
    )

    assert len(orders) == 50


def test_orders_reference_valid_customers():
    orders, _ = generate_orders(
        customer_count=10,
        product_count=5,
        order_count=100,
    )

    assert all(
        1 <= order["customer_id"] <= 10
        for order in orders
    )


def test_order_items_reference_orders():
    orders, order_items = generate_orders(
        customer_count=10,
        product_count=5,
        order_count=50,
    )

    order_ids = {
        order["order_id"]
        for order in orders
    }

    assert all(
        item["order_id"] in order_ids
        for item in order_items
    )


def test_order_items_reference_valid_products():
    _, order_items = generate_orders(
        customer_count=10,
        product_count=5,
        order_count=50,
    )

    assert all(
        1 <= item["product_id"] <= 5
        for item in order_items
    )


def test_order_item_quantity_positive():
    _, order_items = generate_orders(
        customer_count=10,
        product_count=5,
        order_count=50,
    )

    assert all(
        item["quantity"] > 0
        for item in order_items
    )