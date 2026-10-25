import argparse
import hashlib
import json
import math
import random
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parent.parent
CONTRACTS = ROOT / "contracts"
DEFAULT_OUTPUT = ROOT / "data" / "historical_full"

DEFAULT_VEHICLES = 5000
DEFAULT_HOURS = 12
INTERVAL_SECONDS = 30
DEFAULT_CHUNK_SIZE = 50_000
DEFAULT_SEED = 42


def utc_timestamp(value):
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_validator(filename):
    path = CONTRACTS / filename
    if not path.exists():
        raise FileNotFoundError(f"Schema not found: {path}")
    with path.open("r", encoding="utf-8-sig") as file:
        schema = json.load(file)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def validate_record(record, validator):
    error = next(validator.iter_errors(record), None)
    if error is not None:
        raise ValueError(
            f"Invalid record {record.get('event_id', record.get('trip_id'))}: "
            f"{error.message}"
        )


def atomic_write_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as file:
        json.dump(value, file, indent=2)
        file.flush()
    temp.replace(path)


def write_chunk_atomic(path, records):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8", newline="\n") as file:
        for record in records:
            file.write(json.dumps(record, separators=(",", ":")) + "\n")
        file.flush()
    temp.replace(path)


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fingerprint(config):
    raw = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def clamp(value, low, high):
    return max(low, min(value, high))


def normal_speed(previous_speed, rng):
    target = rng.uniform(35.0, 65.0)
    speed = previous_speed + (target - previous_speed) * 0.35
    speed += rng.uniform(-2.0, 2.0)
    return round(clamp(speed, 0.0, 85.0), 2)


def normal_rpm(speed, rng):
    base = 900 + speed * 24
    return int(clamp(base + rng.randint(-100, 100), 900, 2800))


def normal_engine_temp(previous_temp, speed, rpm, rng):
    target = clamp(
        82.0 + speed * 0.10 + max(rpm - 1200, 0) * 0.004,
        80.0,
        102.0,
    )
    value = previous_temp + (target - previous_temp) * 0.18
    value += rng.uniform(-0.25, 0.25)
    return round(clamp(value, 75.0, 105.0), 2)


def normal_cargo_temp(previous_temp, rng):
    value = previous_temp + (4.5 - previous_temp) * 0.12
    value += rng.uniform(-0.12, 0.12)
    return round(clamp(value, 3.0, 6.0), 2)


def normal_fuel_rate(speed, rpm, rng):
    value = 2.5 + speed * 0.065 + max(rpm - 1000, 0) * 0.0015
    value += rng.uniform(-0.3, 0.3)
    return round(clamp(value, 1.5, 12.0), 2)


def route_position(progress, vehicle_number):
    start_lat = 30.0444 + vehicle_number * 0.00015
    start_lon = 31.2357 + vehicle_number * 0.00015
    return (
        start_lat + 0.0556 * progress,
        start_lon + 0.1643 * progress,
    )


def build_trip(n, start, hours):
    return {
        "trip_id": f"trip-{n:05d}",
        "vehicle_id": f"veh-{n:05d}",
        "driver_id": f"driver-{n:05d}",
        "shipment_id": f"ship-{n:05d}",
        "route_id": f"route-{n:05d}",
        "planned_start": utc_timestamp(start),
        "planned_end": utc_timestamp(start + timedelta(hours=hours)),
        "actual_start": utc_timestamp(start),
        "actual_end": None,
        "origin": "Cairo Distribution Center",
        "destination": f"Delivery Zone {(n - 1) % 50 + 1:02d}",
        "trip_status": "in_progress",
        "distance_km": None,
        "fuel_used_liters": None,
    }


def build_state(n):
    return {
        "vehicle_number": n,
        "trip_id": f"trip-{n:05d}",
        "shipment_id": f"ship-{n:05d}",
        "driver_id": f"driver-{n:05d}",
        "route_id": f"route-{n:05d}",
        "previous_speed": 45.0,
        "previous_engine_temp": 88.0,
        "previous_cargo_temp": 4.5,
        "odometer_km": 12000.0 + n * 125,
    }


def event_rng(seed, vehicle_number, interval_index):
    # Stable independent RNG per vehicle/interval makes replay reproducible.
    material = f"{seed}:{vehicle_number}:{interval_index}".encode()
    stable_seed = int.from_bytes(hashlib.sha256(material).digest()[:8], "big")
    return random.Random(stable_seed)


def generate_event(n, interval_index, start, hours, state, seed):
    rng = event_rng(seed, n, interval_index)
    seconds = interval_index * INTERVAL_SECONDS
    events_per_vehicle = hours * 3600 // INTERVAL_SECONDS
    progress = interval_index / max(events_per_vehicle - 1, 1)

    idling = n == 2 and 120 <= seconds < 300
    if idling:
        speed, rpm, fuel_rate = 0.0, 850, 1.2
    else:
        speed = normal_speed(state["previous_speed"], rng)
        rpm = normal_rpm(speed, rng)
        fuel_rate = normal_fuel_rate(speed, rpm, rng)

    engine_temp = normal_engine_temp(
        state["previous_engine_temp"], speed, rpm, rng
    )
    cargo_temp = normal_cargo_temp(state["previous_cargo_temp"], rng)

    if n == 1 and 180 <= seconds < 300:
        cargo_temp = round(
            9.5 + 0.8 * math.sin((seconds - 180) / 120 * math.pi), 2
        )

    latitude, longitude = route_position(progress, n)
    if n == 3 and 240 <= seconds < 360:
        offset = (
            (seconds - 240) / 40
            if seconds <= 280
            else (360 - seconds) / 80
        )
        offset = clamp(offset, 0.0, 1.0)
        latitude += 0.003 * offset
        longitude += 0.003 * offset

    state["odometer_km"] = round(
        state["odometer_km"] + speed * INTERVAL_SECONDS / 3600, 3
    )

    event = {
        "event_id": f"evt-{n:05d}-{interval_index:04d}",
        "event_timestamp": utc_timestamp(
            start + timedelta(seconds=seconds)
        ),
        "vehicle_id": f"veh-{n:05d}",
        "trip_id": state["trip_id"],
        "shipment_id": state["shipment_id"],
        "driver_id": state["driver_id"],
        "route_id": state["route_id"],
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "speed_kph": round(speed, 2),
        "odometer_km": state["odometer_km"],
        "rpm": rpm,
        "fuel_rate_lph": fuel_rate,
        "engine_temp_c": engine_temp,
        "cargo_temp_c": cargo_temp,
        "ignition_on": True,
    }

    state["previous_speed"] = speed
    state["previous_engine_temp"] = engine_temp
    state["previous_cargo_temp"] = cargo_temp
    return event


def verify_manifest(manifest, output, config):
    if manifest.get("config_fingerprint") != fingerprint(config):
        raise ValueError("Saved run configuration fingerprint is invalid.")

    completed_chunks = manifest.get("completed_chunks", [])
    completed_count = manifest.get("completed_events", 0)
    total_events = config["total_events"]
    chunk_size = config["chunk_size"]
    expected_chunks = list(range(len(completed_chunks)))

    if completed_chunks != expected_chunks:
        raise ValueError("Manifest chunk list is not contiguous; refusing resume.")

    expected_completed = min(len(completed_chunks) * chunk_size, total_events)
    if completed_count != expected_completed:
        raise ValueError("Manifest completed_events does not match completed chunks.")

    recorded = {item["index"]: item for item in manifest.get("chunks", [])}
    for idx in completed_chunks:
        chunk_path = output / f"telemetry_{idx:05d}.jsonl"
        metadata = recorded.get(idx)
        if not chunk_path.exists():
            raise FileNotFoundError(f"Committed chunk missing: {chunk_path}")
        if metadata is None or sha256_file(chunk_path) != metadata.get("sha256"):
            raise ValueError(f"Committed chunk checksum mismatch: {chunk_path}")

    return completed_chunks, completed_count


def main():
    parser = argparse.ArgumentParser(
        description="Generate deterministic, chunked historical telemetry with resumable output."
    )
    parser.add_argument("--vehicles", type=int, default=DEFAULT_VEHICLES)
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--start-utc", default=None)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument(
        "--validate-all",
        action="store_true",
        help="Validate every event against JSON Schema (slower).",
    )
    args = parser.parse_args()

    if min(args.vehicles, args.hours, args.chunk_size) <= 0:
        parser.error("vehicles, hours and chunk-size must be positive")
    if args.hours * 3600 % INTERVAL_SECONDS:
        parser.error("hours must produce a whole number of intervals")

    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "manifest.json"

    if args.resume and not manifest_path.exists():
        raise FileNotFoundError(
            f"Cannot resume: no manifest found at {manifest_path}"
        )
    if not args.resume and manifest_path.exists():
        raise FileExistsError(
            f"{manifest_path} exists. Use --resume with the same settings "
            "or select a new --output directory."
        )

    if args.resume:
        with manifest_path.open("r", encoding="utf-8") as file:
            manifest = json.load(file)
        config = manifest["config"]
        requested_start = args.start_utc
        if requested_start is not None:
            requested_start_dt = parse_timestamp(requested_start)
            if utc_timestamp(requested_start_dt) != config["start_utc"]:
                raise ValueError("--start-utc differs from the saved run.")
        requested = {
            "vehicles": args.vehicles,
            "duration_hours": args.hours,
            "interval_seconds": INTERVAL_SECONDS,
            "chunk_size": args.chunk_size,
            "seed": args.seed,
        }
        for key, value in requested.items():
            if config[key] != value:
                raise ValueError(
                    f"Resume setting mismatch for {key}: "
                    f"saved={config[key]}, requested={value}"
                )
        start = parse_timestamp(config["start_utc"])
        if manifest.get("status") == "completed":
            print("This run is already complete; nothing to resume.")
            print(f"Events committed: {manifest['completed_events']:,}")
            print(f"Chunks committed: {len(manifest['completed_chunks']):,}")
            return
    else:
        start = (
            parse_timestamp(args.start_utc)
            if args.start_utc
            else datetime.now(timezone.utc).replace(microsecond=0)
        ).astimezone(timezone.utc)

        events_per_vehicle = args.hours * 3600 // INTERVAL_SECONDS
        total_events = args.vehicles * events_per_vehicle
        config = {
            "vehicles": args.vehicles,
            "duration_hours": args.hours,
            "interval_seconds": INTERVAL_SECONDS,
            "events_per_vehicle": events_per_vehicle,
            "total_events": total_events,
            "chunk_size": args.chunk_size,
            "seed": args.seed,
            "start_utc": utc_timestamp(start),
        }
        manifest = {
            "status": "in_progress",
            "config": config,
            "config_fingerprint": fingerprint(config),
            "completed_chunks": [],
            "completed_events": 0,
            "chunks": [],
        }
        atomic_write_json(manifest_path, manifest)

    total_events = config["total_events"]
    chunk_size = config["chunk_size"]
    events_per_vehicle = config["events_per_vehicle"]
    total_chunks = (total_events + chunk_size - 1) // chunk_size

    telemetry_validator = load_validator("telemetry_event.schema.json")
    trip_validator = load_validator("trip.schema.json")

    trips_path = output / "trips.json"
    if not trips_path.exists():
        trips = [build_trip(n, start, args.hours) for n in range(1, args.vehicles + 1)]
        for trip in trips:
            validate_record(trip, trip_validator)
        atomic_write_json(trips_path, trips)
    else:
        with trips_path.open("r", encoding="utf-8") as file:
            trips = json.load(file)
        if len(trips) != args.vehicles:
            raise ValueError("Existing trips.json does not match this run.")

    completed_chunks, completed_count = verify_manifest(manifest, output, config)

    # Remove only leftover temporary files from atomic writes.
    for temp_path in output.glob("*.tmp"):
        temp_path.unlink()

    states = {n: build_state(n) for n in range(1, args.vehicles + 1)}
    pending = []
    generated = 0
    # Key fix: begin chunk numbering at the committed prefix, not zero.
    chunk_index = len(completed_chunks)

    print(f"Vehicles: {args.vehicles:,}")
    print(f"Duration: {args.hours} hours")
    print(f"Target events: {total_events:,}")
    print(f"Already committed: {completed_count:,}")
    print(f"Output: {output}")

    # Replay from the start to rebuild vehicle state, skipping committed events.
    for interval_index in range(events_per_vehicle):
        for n in range(1, args.vehicles + 1):
            event = generate_event(
                n, interval_index, start, args.hours, states[n], args.seed
            )
            generated += 1

            if generated <= completed_count:
                continue

            if args.validate_all:
                validate_record(event, telemetry_validator)
            pending.append(event)

            if len(pending) == chunk_size or generated == total_events:
                path = output / f"telemetry_{chunk_index:05d}.jsonl"

                # If a prior crash left a renamed chunk not in the manifest,
                # verify it instead of overwriting it.
                if path.exists():
                    with tempfile.NamedTemporaryFile(
                        mode="w",
                        encoding="utf-8",
                        newline="\n",
                        dir=output,
                        suffix=".verify.tmp",
                        delete=False,
                    ) as verify_file:
                        verify_path = Path(verify_file.name)
                        for record in pending:
                            verify_file.write(
                                json.dumps(record, separators=(",", ":")) + "\n"
                            )
                    try:
                        if sha256_file(path) != sha256_file(verify_path):
                            raise ValueError(
                                f"Uncommitted chunk differs from regenerated data: {path}. "
                                "Move it aside and inspect before retrying."
                            )
                    finally:
                        verify_path.unlink(missing_ok=True)
                else:
                    write_chunk_atomic(path, pending)

                checksum = sha256_file(path)
                metadata = {
                    "index": chunk_index,
                    "file": path.name,
                    "events": len(pending),
                    "sha256": checksum,
                }

                if chunk_index == len(manifest["completed_chunks"]):
                    manifest["completed_chunks"].append(chunk_index)
                    manifest["chunks"].append(metadata)
                else:
                    raise RuntimeError(
                        "Unexpected chunk index; manifest prefix is inconsistent."
                    )

                manifest["completed_events"] = min(
                    (chunk_index + 1) * chunk_size, total_events
                )
                manifest["status"] = (
                    "completed"
                    if manifest["completed_events"] == total_events
                    else "in_progress"
                )
                atomic_write_json(manifest_path, manifest)

                print(
                    f"Committed chunk {chunk_index + 1}/{total_chunks}: "
                    f"{len(pending):,} events; "
                    f"{manifest['completed_events']:,}/{total_events:,} total"
                )
                pending.clear()
                chunk_index += 1

    manifest["status"] = (
        "completed"
        if manifest["completed_events"] == total_events
        else "in_progress"
    )
    atomic_write_json(manifest_path, manifest)

    print(f"\nStatus: {manifest['status']}")
    print(f"Events committed: {manifest['completed_events']:,}/{total_events:,}")
    print(f"Chunks committed: {len(manifest['completed_chunks']):,}/{total_chunks:,}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
