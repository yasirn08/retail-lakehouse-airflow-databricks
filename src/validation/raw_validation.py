import json
from pathlib import Path


def load_json(path: str) -> list[dict]:
    with Path(path).open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def validate_required_fields(
    records: list[dict],
    required_fields: list[str],
) -> None:
    if not records:
        raise ValueError("Dataset contains no records.")

    for index, record in enumerate(records):
        missing_fields = [
            field
            for field in required_fields
            if field not in record or record[field] is None
        ]

        if missing_fields:
            raise ValueError(
                f"Record {index} is missing fields: "
                f"{missing_fields}"
            )


def validate_unique_key(
    records: list[dict],
    key: str,
) -> None:
    values = [
        record[key]
        for record in records
    ]

    if len(values) != len(set(values)):
        raise ValueError(
            f"Duplicate values detected for key '{key}'"
        )