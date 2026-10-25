import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = ROOT / "data" / "resume_interrupt_test"
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
    parser = argparse.ArgumentParser(
        description="Validate TrackPulse historical telemetry data"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=(
            "Directory containing manifest.json, trips.json, and telemetry files. "
            f"Default: {DEFAULT_DATA_DIR}"
        ),
    )
    args = parser.parse_args()
    data_dir = args.data_dir.resolve()

    manifest_path = data_dir / "manifest.json"
    trips_path = data_dir / "trips.json"

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    if not trips_path.exists():
        raise FileNotFoundError(f"Trips file not found: {trips_path}")

    with manifest_path.open("r", encoding="utf-8") as file:
        manifest = json.load(file)

    config = manifest.get("config", {})
    # Support both generator manifest naming conventions used in the project.
    expected_total = config.get("total_events", config.get("target_events"))
    expected_vehicles = config.get("vehicles")
    expected_chunk_count = len(manifest.get("completed_chunks", []))
    manifest_chunks = manifest.get("chunks", [])

    if expected_total is None:
        raise ValueError(
            "manifest.json is missing config.total_events or config.target_events"
        )
    if manifest.get("status") != "completed":
        raise ValueError(
            f"Dataset is not complete; manifest status is {manifest.get('status')!r}"
        )

    # If the manifest records chunk metadata in a different field, use it only
    # when it is a list; this keeps validation explicit rather than guessing.
    if not manifest_chunks and isinstance(manifest.get("completed_chunks"), list):
        manifest_chunks = manifest["completed_chunks"]

    telemetry_validator = load_validator("telemetry_event.schema.json")
    trip_validator = load_validator("trip.schema.json")

    with trips_path.open("r", encoding="utf-8") as file:
        trips = json.load(file)

    trip_ids = set()
    vehicle_ids = set()

    for trip in trips:
        errors = list(trip_validator.iter_errors(trip))
        if errors:
            raise ValueError(
                f"Invalid trip {trip.get('trip_id')}: {errors[0].message}"
            )
        trip_ids.add(trip["trip_id"])
        vehicle_ids.add(trip["vehicle_id"])

    if expected_vehicles is not None and len(trips) != expected_vehicles:
        raise ValueError(
            f"Manifest says {expected_vehicles} vehicles, "
            f"but trips.json contains {len(trips)} trips"
        )

    event_ids = set()
    counts_by_vehicle = defaultdict(int)
    last_timestamp = {}
    last_odometer = {}
    total = 0

    # The generator writes chunks under telemetry/ in the configured version;
    # support a flat directory too for older output layouts.
    telemetry_dir = data_dir / "telemetry"
    if telemetry_dir.is_dir():
        files = sorted(telemetry_dir.glob("telemetry_*.jsonl"))
        if not files:
            files = sorted(telemetry_dir.glob("*.jsonl"))
    else:
        files = sorted(data_dir.glob("telemetry_*.jsonl"))

    if not files:
        raise FileNotFoundError(
            f"No telemetry JSONL files found under {telemetry_dir if telemetry_dir.is_dir() else data_dir}"
        )

    # Normalize manifest chunk metadata into a map keyed by chunk index.
    chunks_by_index = {}
    for position, item in enumerate(manifest_chunks):
        if not isinstance(item, dict):
            continue
        index = item.get("index", position)
        chunks_by_index[index] = item

    # If manifest only has completed_chunks metadata, use it for the count.
    if manifest_chunks:
        expected_chunk_count = len(manifest_chunks)

    if expected_chunk_count and len(files) != expected_chunk_count:
        raise ValueError(
            f"Manifest lists {expected_chunk_count} completed chunks, "
            f"but found {len(files)} telemetry files"
        )

    for position, path in enumerate(files):
        metadata = chunks_by_index.get(position)
        if metadata is not None:
            recorded_name = metadata.get("file") or metadata.get("filename")
            if recorded_name and recorded_name not in {path.name, str(path.relative_to(data_dir))}:
                raise ValueError(
                    f"Manifest filename mismatch for chunk {position}: "
                    f"manifest={recorded_name}, actual={path.name}"
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
                if errors:
                    raise ValueError(
                        f"{path.name}:{line_number}: {errors[0].message}"
                    )

                event_id = event["event_id"]
                vehicle_id = event["vehicle_id"]
                event_time = event["event_timestamp"]
                odometer = event["odometer_km"]

                if event_id in event_ids:
                    raise ValueError(f"Duplicate event_id: {event_id}")
                event_ids.add(event_id)

                if vehicle_id not in vehicle_ids:
                    raise ValueError(f"Unknown vehicle: {vehicle_id}")
                if event["trip_id"] not in trip_ids:
                    raise ValueError(f"Unknown trip: {event['trip_id']}")

                if vehicle_id in last_timestamp:
                    if event_time < last_timestamp[vehicle_id]:
                        raise ValueError(f"Timestamp went backwards for {vehicle_id}")

                if vehicle_id in last_odometer:
                    if odometer < last_odometer[vehicle_id]:
                        raise ValueError(f"Odometer decreased for {vehicle_id}")

                last_timestamp[vehicle_id] = event_time
                last_odometer[vehicle_id] = odometer
                counts_by_vehicle[vehicle_id] += 1
                total += 1

    if total != expected_total:
        raise ValueError(
            f"Manifest says {expected_total} events, but found {total}"
        )

    expected_events_per_vehicle = config.get("events_per_vehicle")
    if expected_events_per_vehicle is not None:
        for vehicle_id in vehicle_ids:
            actual = counts_by_vehicle.get(vehicle_id, 0)
            if actual != expected_events_per_vehicle:
                raise ValueError(
                    f"{vehicle_id} has {actual} events; "
                    f"expected {expected_events_per_vehicle}"
                )

    # Compare recorded chunk checksums/counts when those fields are available.
    checksum_checks = 0
    count_checks = 0
    for position, path in enumerate(files):
        metadata = chunks_by_index.get(position)
        if metadata is None:
            continue

        expected_sha = metadata.get("sha256") or metadata.get("checksum")
        if expected_sha:
            digest = hashlib.sha256()
            with path.open("rb") as file:
                for block in iter(lambda: file.read(1024 * 1024), b""):
                    digest.update(block)
            if digest.hexdigest() != expected_sha:
                raise ValueError(f"Checksum mismatch for {path.name}")
            checksum_checks += 1

        expected_chunk_events = metadata.get("events", metadata.get("event_count"))
        if expected_chunk_events is not None:
            with path.open("r", encoding="utf-8") as file:
                file_event_count = sum(1 for line in file if line.strip())
            if file_event_count != expected_chunk_events:
                raise ValueError(
                    f"{path.name}: manifest says {expected_chunk_events} events, "
                    f"found {file_event_count}"
                )
            count_checks += 1

    print("PASS: Trip schema")
    print("PASS: Telemetry schema")
    print("PASS: Unique event IDs")
    print("PASS: Vehicle and trip relationships")
    print("PASS: Per-vehicle timestamp order")
    print("PASS: Odometer never decreases")
    print("PASS: Manifest event and chunk counts")
    if count_checks:
        print(f"PASS: Chunk event counts ({count_checks:,} files checked)")
    if checksum_checks:
        print(f"PASS: SHA-256 checksums ({checksum_checks:,} files checked)")
    print(f"Total events: {total:,}")
    print(f"Vehicles with events: {len(counts_by_vehicle):,}")
    print(f"Telemetry files: {len(files):,}")
    print(f"Validated data directory: {data_dir}")


if __name__ == "__main__":
    main()
