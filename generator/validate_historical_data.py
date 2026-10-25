import json
from collections import defaultdict
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "resume_interrupt_test"
CONTRACTS = ROOT / "contracts"


def load_validator(filename):
    schema_path = CONTRACTS / filename
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema not found: {schema_path}")

    with schema_path.open("r", encoding="utf-8-sig") as file:
        schema = json.load(file)

    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
    )


def main():
    manifest_path = DATA_DIR / "manifest.json"
    trips_path = DATA_DIR / "trips.json"

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    if not trips_path.exists():
        raise FileNotFoundError(f"Trips file not found: {trips_path}")

    with manifest_path.open("r", encoding="utf-8") as file:
        manifest = json.load(file)

    config = manifest.get("config", {})
    expected_total = config.get("total_events")
    expected_vehicles = config.get("vehicles")
    expected_chunk_count = len(manifest.get("completed_chunks", []))
    manifest_chunks = manifest.get("chunks", [])

    if expected_total is None:
        raise ValueError("manifest.json is missing config.total_events")
    if manifest.get("status") != "completed":
        raise ValueError(
            f"Dataset is not complete; manifest status is "
            f"{manifest.get('status')!r}"
        )

    telemetry_validator = load_validator("telemetry_event.schema.json")
    trip_validator = load_validator("trip.schema.json")

    with trips_path.open("r", encoding="utf-8") as file:
        trips = json.load(file)

    trip_ids = set()
    vehicle_ids = set()

    for trip in trips:
        errors = list(trip_validator.iter_errors(trip))
        assert not errors, (
            f"Invalid trip {trip.get('trip_id')}: {errors[0].message}"
        )
        trip_ids.add(trip["trip_id"])
        vehicle_ids.add(trip["vehicle_id"])

    if expected_vehicles is not None:
        assert len(trips) == expected_vehicles, (
            f"Manifest says {expected_vehicles} vehicles, "
            f"but trips.json contains {len(trips)} trips"
        )

    event_ids = set()
    counts_by_vehicle = defaultdict(int)
    last_timestamp = {}
    last_odometer = {}
    total = 0

    files = sorted(DATA_DIR.glob("telemetry_*.jsonl"))
    assert len(files) == expected_chunk_count, (
        f"Manifest lists {expected_chunk_count} completed chunks, "
        f"but found {len(files)} telemetry files"
    )

    # Verify file names/order and checksums recorded in the manifest.
    chunks_by_index = {
        item["index"]: item for item in manifest_chunks
    }
    for index, path in enumerate(files):
        assert path.name == f"telemetry_{index:05d}.jsonl", (
            f"Unexpected telemetry file name: {path.name}"
        )
        metadata = chunks_by_index.get(index)
        assert metadata is not None, (
            f"Manifest has no metadata for chunk index {index}"
        )
        assert metadata.get("file") == path.name, (
            f"Manifest filename mismatch for chunk index {index}"
        )

    for path in files:
        with path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                if not line.strip():
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid JSON at {path.name}:{line_number}: {exc}"
                    ) from exc

                errors = list(telemetry_validator.iter_errors(event))
                assert not errors, (
                    f"{path.name}:{line_number}: {errors[0].message}"
                )

                event_id = event["event_id"]
                vehicle_id = event["vehicle_id"]
                event_time = event["event_timestamp"]
                odometer = event["odometer_km"]

                assert event_id not in event_ids, (
                    f"Duplicate event_id: {event_id}"
                )
                event_ids.add(event_id)

                assert vehicle_id in vehicle_ids, (
                    f"Unknown vehicle: {vehicle_id}"
                )
                assert event["trip_id"] in trip_ids, (
                    f"Unknown trip: {event['trip_id']}"
                )

                if vehicle_id in last_timestamp:
                    assert event_time >= last_timestamp[vehicle_id], (
                        f"Timestamp went backwards for {vehicle_id}"
                    )

                if vehicle_id in last_odometer:
                    assert odometer >= last_odometer[vehicle_id], (
                        f"Odometer decreased for {vehicle_id}"
                    )

                last_timestamp[vehicle_id] = event_time
                last_odometer[vehicle_id] = odometer
                counts_by_vehicle[vehicle_id] += 1
                total += 1

    assert total == expected_total, (
        f"Manifest says {expected_total} events, but found {total}"
    )

    # Each vehicle should have exactly one event per configured interval.
    expected_events_per_vehicle = config.get("events_per_vehicle")
    if expected_events_per_vehicle is not None:
        for vehicle_id in vehicle_ids:
            actual = counts_by_vehicle.get(vehicle_id, 0)
            assert actual == expected_events_per_vehicle, (
                f"{vehicle_id} has {actual} events; "
                f"expected {expected_events_per_vehicle}"
            )

    # Compare recorded chunk event counts and checksums.
    for index, path in enumerate(files):
        metadata = chunks_by_index[index]
        with path.open("rb") as file:
            import hashlib

            digest = hashlib.sha256()
            for block in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(block)

        assert digest.hexdigest() == metadata.get("sha256"), (
            f"Checksum mismatch for {path.name}"
        )

        with path.open("r", encoding="utf-8") as file:
            file_event_count = sum(1 for line in file if line.strip())

        assert file_event_count == metadata.get("events"), (
            f"{path.name}: manifest says {metadata.get('events')} events, "
            f"found {file_event_count}"
        )

    assert len(event_ids) == total, "Event IDs are not unique"

    print("PASS: Trip schema")
    print("PASS: Telemetry schema")
    print("PASS: Unique event IDs")
    print("PASS: Vehicle and trip relationships")
    print("PASS: Per-vehicle timestamp order")
    print("PASS: Odometer never decreases")
    print("PASS: Manifest event and chunk counts")
    print("PASS: Chunk filenames, event counts, and SHA-256 checksums")
    print(f"Total events: {total:,}")
    print(f"Vehicles with events: {len(counts_by_vehicle):,}")
    print(f"Telemetry files: {len(files):,}")


if __name__ == "__main__":
    main()
