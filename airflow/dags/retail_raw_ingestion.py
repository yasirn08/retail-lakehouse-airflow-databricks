from src.validation.raw_validation import (
    load_json,
    validate_required_fields,
    validate_unique_key,
)
from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG, task

from src.extractors.retail_generator import (
    generate_customers,
    generate_orders,
    generate_products,
)
from src.utils.file_utils import write_json


RAW_BASE_PATH = "/opt/airflow/data/raw"


with DAG(
    dag_id="retail_raw_ingestion",
    description="Generate and ingest raw retail data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail", "ingestion", "portfolio"],
) as dag:

    @task
    def create_run_directory() -> str:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")

        output_path = f"{RAW_BASE_PATH}/{run_date}"

        Path(output_path).mkdir(
            parents=True,
            exist_ok=True,
        )

        return output_path

    @task
    def extract_customers(output_path: str) -> dict:
        customers = generate_customers(
            count=100,
        )

        file_path = f"{output_path}/customers.json"

        write_json(
            customers,
            file_path,
        )

        return {
            "dataset": "customers",
            "rows": len(customers),
            "path": file_path,
        }

    @task
    def extract_products(output_path: str) -> dict:
        products = generate_products(
            count=50,
        )

        file_path = f"{output_path}/products.json"

        write_json(
            products,
            file_path,
        )

        return {
            "dataset": "products",
            "rows": len(products),
            "path": file_path,
        }

    @task
    def extract_orders(output_path: str) -> list[dict]:
        orders, order_items = generate_orders(
            customer_count=100,
            product_count=50,
            order_count=500,
        )

        orders_path = f"{output_path}/orders.json"
        items_path = f"{output_path}/order_items.json"

        write_json(
            orders,
            orders_path,
        )

        write_json(
            order_items,
            items_path,
        )

        return [
            {
                "dataset": "orders",
                "rows": len(orders),
                "path": orders_path,
            },
            {
                "dataset": "order_items",
                "rows": len(order_items),
                "path": items_path,
            },
        ]
    @task
    def validate_raw_data(
        customers: dict,
        products: dict,
        order_data: list[dict],
    ) -> None:
        customer_records = load_json(
            customers["path"],
        )

        product_records = load_json(
            products["path"],
        )

        order_records = load_json(
            order_data[0]["path"],
        )

        order_item_records = load_json(
            order_data[1]["path"],
        )

        validate_required_fields(
            customer_records,
            [
                "customer_id",
                "customer_name",
                "email",
                "city",
            ],
        )

        validate_unique_key(
            customer_records,
            "customer_id",
        )

        validate_required_fields(
            product_records,
            [
                "product_id",
                "product_name",
                "category",
                "price",
            ],
        )

        validate_unique_key(
            product_records,
            "product_id",
        )

        validate_required_fields(
            order_records,
            [
                "order_id",
                "customer_id",
                "order_date",
                "status",
            ],
        )

        validate_unique_key(
            order_records,
            "order_id",
        )

        validate_required_fields(
            order_item_records,
            [
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
            ],
        )

        print("All raw data quality checks passed.")

    @task
    def summarize_ingestion(
        customers: dict,
        products: dict,
        order_data: list[dict],
    ) -> None:
        datasets = [
            customers,
            products,
            *order_data,
        ]

        print("Retail ingestion completed")

        for dataset in datasets:
            print(
                f"{dataset['dataset']}: "
                f"{dataset['rows']} rows "
                f"-> {dataset['path']}"
            )

    run_directory = create_run_directory()

    customer_result = extract_customers(
        run_directory,
    )

    product_result = extract_products(
        run_directory,
    )

    order_result = extract_orders(
        run_directory,
    )

    quality_check = validate_raw_data(
        customer_result,
        product_result,
        order_result,
    )

    summary = summarize_ingestion(
        customer_result,
        product_result,
        order_result,
    )

    quality_check >> summary