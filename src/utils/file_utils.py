import json
from pathlib import Path


def write_json(data: list[dict], output_path: str) -> None:
    path = Path(output_path)

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)