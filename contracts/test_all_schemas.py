import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


CONTRACTS_DIR = Path(__file__).resolve().parent

SCHEMA_FILES = [
    "telemetry_event.schema.json",
    "trip.schema.json",
    "cold_chain_breach.schema.json",
]

for filename in SCHEMA_FILES:
    schema_path = CONTRACTS_DIR / filename

    with schema_path.open("r", encoding="utf-8-sig") as file:
        schema = json.load(file)

    Draft202012Validator.check_schema(schema)

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
    )

    print(f"PASS: {filename} is a valid JSON Schema")

print("\nAll schema definitions passed validation.")
