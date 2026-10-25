from __future__ import annotations



import argparse
import csv

import hashlib

import json

import math

import os

import random

import shutil

import sys

import tempfile

from datetime import datetime, timedelta, timezone

from pathlib import Path

from typing import Any



ROOT = Path(__file__).resolve().parents[1]

DEFAULT_OUT = ROOT / "data" / "historical_v2"

DEFAULT_VEHICLES = 5000

DEFAULT_TARGET_EVENTS = 15_000_000

DEFAULT_INTERVAL = 30

DEFAULT_CHUNK = 50_000

SCHEMA_PATH = ROOT / "contracts" / "telemetry_event.schema.json"

TRIP_SCHEMA_PATH = ROOT / "contracts" / "trip.schema.json"



# Approximate road corridors/waypoints, not a routable road-network map.

ROUTES = [

    ("cairo_ring", [(30.0444,31.2357),(30.0131,31.2089),(29.9820,31.1342),(30.0074,31.4200),(30.0444,31.2357)]),

    ("new_cairo", [(30.0444,31.2357),(30.0074,31.4200),(30.0276,31.4913),(30.0131,31.4518),(30.0444,31.2357)]),

    ("alex_desert", [(30.0444,31.2357),(30.1200,30.9500),(30.8200,29.6000),(31.2001,29.9187)]),

    ("ismailia", [(30.0444,31.2357),(30.1500,31.5000),(30.5900,32.2700),(30.5965,32.2715)]),

    ("suez", [(30.0444,31.2357),(29.9500,31.5000),(29.9000,32.5000),(29.9668,32.5498)]),

    ("fayoum", [(30.0444,31.2357),(29.9500,31.1500),(29.5000,30.8000),(29.3084,30.8428)]),

    ("beni_suef", [(30.0444,31.2357),(29.8000,31.2000),(29.0661,31.0994)]),

    ("assuit", [(30.0444,31.2357),(29.3000,31.0000),(27.1800,31.1837)]),

    ("port_said", [(30.0444,31.2357),(30.3000,32.0000),(31.2565,32.2841)]),

    ("north_cairo", [(30.0444,31.2357),(30.1500,31.2500),(30.2500,31.2000),(30.0444,31.2357)]),

]

CARGO_MIN, CARGO_MAX = -5.0, 5.0





def atomic_json(path: Path, obj: Any) -> None:

    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")

    with tmp.open("w", encoding="utf-8", newline="\n") as f:

        json.dump(obj, f, separators=(",", ":"), ensure_ascii=False)

        f.write("\n")

        f.flush()

        os.fsync(f.fileno())

    os.replace(tmp, path)





def sha256_file(path: Path) -> str:

    h = hashlib.sha256()

    with path.open("rb") as f:

        for block in iter(lambda: f.read(1024 * 1024), b""):

            h.update(block)

    return h.hexdigest()





def parse_start(value: str | None) -> datetime:

    if value is None:

        return datetime.now(timezone.utc).replace(microsecond=0)

    s = value.strip().replace("Z", "+00:00")

    dt = datetime.fromisoformat(s)

    if dt.tzinfo is None:

        raise argparse.ArgumentTypeError("--start-utc must include timezone, e.g. 2026-01-01T00:00:00Z")

    return dt.astimezone(timezone.utc).replace(microsecond=0)





def interp_route(waypoints: list[tuple[float, float]], progress: float) -> tuple[float, float]:

    # progress loops smoothly along a waypoint polyline. The route is a stylized corridor.

    lengths = []

    total = 0.0

    for (a, b), (c, d) in zip(waypoints, waypoints[1:]):

        length = math.hypot((c-a) * 111.0, (d-b) * 85.0)

        lengths.append(length)

        total += length

    if total <= 0:

        return waypoints[0]

    target = (progress % total)

    for i, length in enumerate(lengths):

        if target <= length or i == len(lengths)-1:

            frac = 0.0 if length == 0 else target / length

            a, b = waypoints[i]

            c, d = waypoints[i+1]

            return a + (c-a)*frac, b + (d-b)*frac

        target -= length

    return waypoints[-1]





def make_profile(vehicle_num: int, seed: int) -> dict[str, Any]:

    r = random.Random(seed * 1_000_003 + vehicle_num * 9176)

    route_idx = r.randrange(len(ROUTES))

    kind = r.choices(["van", "truck", "refrigerated"], weights=[0.22, 0.48, 0.30])[0]

    return {

        "vehicle_id": f"VEH-{vehicle_num:05d}",

        "driver_id": f"DRV-{((vehicle_num - 1) % max(1, int(vehicle_num * 0.72)) + 1):05d}",

        "route_id": f"RTE-{route_idx+1:03d}",

        "route_idx": route_idx,

        "kind": kind,

        "cruise": r.uniform(42, 78) if kind == "truck" else r.uniform(35, 88),

        "fuel_factor": r.uniform(0.75, 1.35) * (1.22 if kind == "truck" else 1.0),

        "engine_base": r.uniform(78, 91),

        "cargo_target": r.uniform(-1.5, 2.0),

        "phase": r.random() * math.tau,

        "speed": r.uniform(0, 12),

        "rpm": r.uniform(700, 1100),

        "engine_temp": r.uniform(72, 84),

        "cargo_temp": r.uniform(-1.0, 2.0),

        "odometer": r.uniform(20_000, 450_000),

        "route_progress_km": r.uniform(0, 100),

        "trip_num": 1,

        "last_timestamp": None,

        "rng_seed": seed * 1_000_003 + vehicle_num * 9176,

        "event_seq": 0,

    }





def build_trip(p: dict[str, Any], start: datetime, duration_seconds: float, seed: int) -> dict[str, Any]:

    end = start + timedelta(seconds=duration_seconds)

    n = int(p["vehicle_id"].split("-")[1])

    r = random.Random(seed + n * 31)

    status = "in_progress"

    return {

        "trip_id": f"TRIP-{n:05d}-001",

        "vehicle_id": p["vehicle_id"],

        "driver_id": p["driver_id"],

        "shipment_id": f"SHP-{n:05d}",

        "route_id": p["route_id"],

        "planned_start": start.isoformat().replace("+00:00", "Z"),

        "planned_end": end.isoformat().replace("+00:00", "Z"),

        "trip_status": status,

        "actual_start": start.isoformat().replace("+00:00", "Z"),

        "actual_end": None,

        "distance_km": None,

        "fuel_used_liters": None,

        "origin": ROUTES[p["route_idx"]][0],

        "destination": ROUTES[p["route_idx"]][1][-1],

    }





def make_event(p: dict[str, Any], interval_idx: int, start: datetime, interval_seconds: int,

               seed: int, total_intervals_est: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:

    r = random.Random(p["rng_seed"] + interval_idx * 104729)

    timestamp = start + timedelta(seconds=interval_idx * interval_seconds)

    phase = p["phase"] + interval_idx / 180.0

    # Slow-down / stop waves are correlated over time, not independent white noise.

    traffic = 0.60 + 0.25 * math.sin(phase / 4.2) + 0.15 * math.sin(phase / 13.0)

    stop_chance = 0.035 + max(0.0, 0.35 - traffic) * 0.25

    if r.random() < stop_chance:

        target_speed = r.uniform(0, 3)

    else:

        target_speed = p["cruise"] * max(0.18, min(1.12, traffic + r.uniform(-0.12, 0.12)))

    prev_speed = p["speed"]

    speed = max(0.0, min(105.0, prev_speed * 0.68 + target_speed * 0.32 + r.uniform(-2.0, 2.0)))

    if speed < 1.3:

        speed = 0.0

    accel = (speed - prev_speed) / max(1, interval_seconds)

    rpm_target = 700 + speed * (28 if p["kind"] == "truck" else 24) + max(0, accel) * 280

    rpm = int(round(max(650.0, min(3200.0, p["rpm"] * 0.55 + rpm_target * 0.45 + r.uniform(-70, 70)))))

    # Fuel rate depends on vehicle type, speed, engine load and acceleration.

    fuel = (0.9 + speed * 0.060 + rpm / 3200.0 + max(0, accel) * 1.0) * p["fuel_factor"]

    engine_target = p["engine_base"] + speed * 0.10 + max(0, accel) * 1.4

    engine_temp = max(65.0, min(110.0, p["engine_temp"] * 0.94 + engine_target * 0.06 + r.uniform(-0.30, 0.30)) )

    cargo_target = p["cargo_target"]

    cargo_temp = p["cargo_temp"] * 0.985 + cargo_target * 0.015 + r.uniform(-0.06, 0.06)

    p["speed"], p["rpm"], p["engine_temp"], p["cargo_temp"] = speed, rpm, engine_temp, cargo_temp



    distance = speed * interval_seconds / 3600.0

    p["odometer"] += distance

    p["route_progress_km"] += distance

    waypoints = ROUTES[p["route_idx"]][1]

    lat, lon = interp_route(waypoints, p["route_progress_km"])

    lat += r.uniform(-0.00004, 0.00004)

    lon += r.uniform(-0.00004, 0.00004)



    # Planned synthetic incidents at repeatable points in the simulated timeline.

    incident_windows = {

        max(5, int(total_intervals_est * 0.22)): ("INC-COLLISION-A", "collision", 17, 18),

        max(10, int(total_intervals_est * 0.68)): ("INC-COLLISION-B", "collision", 2401, 2402),

    }

    incident_at = None

    for at, info in incident_windows.items():

        if interval_idx in (at, at + 1, at + 2):

            incident_at = (info, at)

            break
    # Deterministic fault injection: target ~15% of telemetry events, not 15% per anomaly type.
    fault_rng = random.Random(p["rng_seed"] + interval_idx * 7919 + 0x5A17)
    injected_fault = None
    if fault_rng.random() < 0.15:
        choices = ["fuel_anomaly", "engine_overheat", "harsh_braking", "excessive_idling", "route_deviation"]
        weights = [0.22, 0.16, 0.20, 0.18, 0.12]
        if p["kind"] == "refrigerated":
            choices.append("cargo_temp_breach")
            weights.append(0.12)
        injected_fault = fault_rng.choices(choices, weights=weights, k=1)[0]
        if injected_fault == "fuel_anomaly":
            fuel *= 2.1
        elif injected_fault == "engine_overheat":
            engine_temp = fault_rng.uniform(108.0, 118.0)
        elif injected_fault == "harsh_braking":
            speed = max(0.0, prev_speed - fault_rng.uniform(18.0, 32.0))
            accel = (speed - prev_speed) / max(1, interval_seconds)
        elif injected_fault == "excessive_idling":
            speed = 0.0
            rpm = fault_rng.randint(750, 1050)
            fuel = max(fuel, fault_rng.uniform(1.8, 3.2))
        elif injected_fault == "cargo_temp_breach":
            cargo_temp = fault_rng.uniform(7.0, 10.0)
        elif injected_fault == "route_deviation":
            lat += fault_rng.choice([-1, 1]) * fault_rng.uniform(0.015, 0.035)
            lon += fault_rng.choice([-1, 1]) * fault_rng.uniform(0.015, 0.035)




    event_id = f"{p['vehicle_id']}-{interval_idx:07d}"

    event = {

        "event_id": event_id,

        "event_timestamp": timestamp.isoformat().replace("+00:00", "Z"),

        "vehicle_id": p["vehicle_id"],

        "latitude": round(lat, 6),

        "longitude": round(lon, 6),

        "speed_kph": round(speed, 2),

        "odometer_km": round(p["odometer"], 3),

        "rpm": int(rpm),

        "fuel_rate_lph": round(fuel, 3),

        "engine_temp_c": round(engine_temp, 2),

        "ignition_on": bool(speed > 0.5 or injected_fault == "excessive_idling"),

        "trip_id": f"TRIP-{int(p['vehicle_id'].split('-')[1]):05d}-001",

        "shipment_id": f"SHP-{int(p['vehicle_id'].split('-')[1]):05d}",

        "driver_id": p["driver_id"],

        "route_id": p["route_id"],

        "cargo_temp_c": round(cargo_temp, 2) if p["kind"] == "refrigerated" else None,

    }

    labels: list[dict[str, Any]] = []

    def label(kind: str, severity: str, details: str, incident_id: str | None = None) -> None:

        labels.append({

            "event_id": event_id, "event_timestamp": event["event_timestamp"],

            "vehicle_id": p["vehicle_id"], "anomaly_type": kind,

            "severity": severity, "details": details, "incident_id": incident_id

        })

    if injected_fault == "engine_overheat":
        label("engine_overheat", "critical" if engine_temp >= 115 else "high", f"OBD-II engine_temp_c={engine_temp:.2f}; synthetic overheat injected")
    elif injected_fault == "fuel_anomaly":
        label("fuel_anomaly", "high", f"OBD-II fuel_rate_lph={fuel:.3f}; injected excess consumption")
    elif injected_fault == "harsh_braking":
        label("harsh_braking", "high", f"synthetic abrupt speed drop: {prev_speed-speed:.2f} kph over {interval_seconds}s")
    elif injected_fault == "excessive_idling":
        label("excessive_idling", "medium", f"speed_kph=0; ignition_on=True; rpm={rpm}; elevated idle fuel consumption")
    elif injected_fault == "cargo_temp_breach":
        label("cargo_temp_breach", "high", f"refrigeration cargo_temp_c={cargo_temp:.2f}; allowed range=[{CARGO_MIN},{CARGO_MAX}]")
    elif injected_fault == "route_deviation":
        event["latitude"], event["longitude"] = round(lat, 6), round(lon, 6)
        label("route_deviation", "high", "synthetic GPS position deviated from assigned route corridor")

    if incident_at:

        info, incident_start = incident_at

        incident_id, kind, va, vb = info

        vehicle_num = int(p["vehicle_id"].split("-")[1])

        if vehicle_num in (va, vb):

            # Place both vehicles at a common collision location, away from the route line.

            event["latitude"] = round(30.055 + (0.00015 if vehicle_num == vb else 0), 6)

            event["longitude"] = round(31.245 + (0.00015 if vehicle_num == vb else 0), 6)

            event["speed_kph"] = round(max(0.0, speed * (0.1 if interval_idx == incident_start else 0.0)), 2)

            label("collision", "critical", "simulated vehicle collision", incident_id)

    p["event_seq"] = interval_idx + 1

    p["last_timestamp"] = event["event_timestamp"]

    return event, labels





def validate_with_schema(event: dict[str, Any], schema: dict[str, Any]) -> None:

    # Avoid mandatory jsonschema dependency for the hot path; full validation is optional.

    try:

        import jsonschema

    except ImportError as exc:

        raise RuntimeError("Install requirements first: pip install -r requirements.txt") from exc

    jsonschema.validate(event, schema)





def load_json(path: Path, default: Any) -> Any:

    if not path.exists():

        return default

    with path.open(encoding="utf-8-sig") as f:

        return json.load(f)






def validate_master_data(master_dir: Path, states: list[dict[str, Any]]) -> None:
    """Validate generated profile IDs/types/assignments against the master CSVs."""
    required = {
        "vehicles.csv": {"vehicle_id", "vehicle_type"},
        "drivers.csv": {"driver_id"},
        "routes.csv": {"route_id"},
        "shipments.csv": {"shipment_id"},
        "trip_plans.csv": {"trip_id", "vehicle_id", "driver_id", "shipment_id", "route_id"},
    }
    loaded: dict[str, list[dict[str, str]]] = {}
    for filename, columns in required.items():
        path = master_dir / filename
        if not path.exists():
            raise SystemExit(f"Master data file missing: {path}")
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            raise SystemExit(f"Master data file is empty: {path}")
        missing = columns - set(rows[0].keys())
        if missing:
            raise SystemExit(f"{path.name} is missing columns: {sorted(missing)}")
        loaded[filename] = rows

    vehicle_rows = {r["vehicle_id"]: r for r in loaded["vehicles.csv"]}
    driver_ids = {r["driver_id"] for r in loaded["drivers.csv"]}
    route_ids = {r["route_id"] for r in loaded["routes.csv"]}
    shipment_rows = {r["shipment_id"]: r for r in loaded["shipments.csv"]}
    trip_rows = {r["vehicle_id"]: r for r in loaded["trip_plans.csv"]}

    if len(vehicle_rows) != len(loaded["vehicles.csv"]):
        raise SystemExit("vehicles.csv contains duplicate vehicle_id values.")
    if len(states) != len(vehicle_rows):
        raise SystemExit(
            f"Vehicle count mismatch: generator has {len(states)}, "
            f"master data has {len(vehicle_rows)}."
        )

    for p in states:
        vid = p["vehicle_id"]
        if vid not in vehicle_rows:
            raise SystemExit(f"{vid} from generator is missing in vehicles.csv.")
        vr = vehicle_rows[vid]
        # The simulation profile type must match the master record; do not silently override.
        if vr["vehicle_type"] != p["kind"]:
            raise SystemExit(
                f"vehicle_type mismatch for {vid}: generator={p['kind']}, "
                f"master={vr['vehicle_type']}. Generate master data with the same --seed."
            )
        if p["driver_id"] not in driver_ids:
            raise SystemExit(f"Driver {p['driver_id']} for {vid} is missing in drivers.csv.")
        if p["route_id"] not in route_ids:
            raise SystemExit(f"Route {p['route_id']} for {vid} is missing in routes.csv.")
        tr = trip_rows.get(vid)
        sr = shipment_rows.get(tr.get("shipment_id")) if tr else None
        if not sr or not tr:
            raise SystemExit(f"Missing shipment/trip plan for {vid}.")
        expected = {
            "driver_id": p["driver_id"],
            "route_id": p["route_id"],
        }
        for key, value in expected.items():
            if tr.get(key) != value:
                raise SystemExit(
                    f"{key} assignment mismatch for {vid}: generator={value}, "
                    f"trip_plans.csv={tr.get(key)}."
                )
        if sr.get("shipment_id") != tr.get("shipment_id"):
            raise SystemExit(f"Shipment ID mismatch between shipment/trip plan for {vid}.")
        if tr.get("trip_id") != f"TRIP-{int(vid.split('-')[-1]):05d}-001":
            raise SystemExit(f"Unexpected trip_id for {vid} in trip_plans.csv.")


def main() -> int:

    ap = argparse.ArgumentParser(description="Fast, resumable TrackPulse historical data generator")

    ap.add_argument("--vehicles", type=int, default=DEFAULT_VEHICLES)

    ap.add_argument("--target-events", type=int, default=DEFAULT_TARGET_EVENTS)

    ap.add_argument("--interval-seconds", type=int, default=DEFAULT_INTERVAL)

    ap.add_argument("--start-utc", default=None, help="e.g. 2026-01-01T00:00:00Z; default is current UTC")

    ap.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK, help="must be a multiple of --vehicles")

    ap.add_argument("--seed", type=int, default=42)

    ap.add_argument("--output", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--master-data", type=Path, required=True, help="Directory containing vehicles.csv, drivers.csv, routes.csv, shipments.csv, trip_plans.csv")

    ap.add_argument("--resume", action="store_true")

    ap.add_argument("--validate-all", action="store_true", help="validate every telemetry row with the JSON schema; slower")

    args = ap.parse_args()



    if args.vehicles < 1 or args.target_events < 1 or args.interval_seconds < 1:

        ap.error("vehicles, target-events, and interval-seconds must be positive")

    if args.chunk_size < args.vehicles or args.chunk_size % args.vehicles:

        ap.error("--chunk-size must be >= --vehicles and an exact multiple of --vehicles")

    start = parse_start(args.start_utc)

    # Whole fleet intervals ensure every vehicle has the same number of observations.

    target_events = math.ceil(args.target_events / args.vehicles) * args.vehicles

    target_intervals = target_events // args.vehicles

    duration_seconds = max(0, target_intervals - 1) * args.interval_seconds

    out = args.output.resolve()

    out.mkdir(parents=True, exist_ok=True)

    telemetry_dir = out / "telemetry"

    anomaly_dir = out / "anomaly_chunks"

    telemetry_dir.mkdir(exist_ok=True)

    anomaly_dir.mkdir(exist_ok=True)

    manifest_path, checkpoint_path = out / "manifest.json", out / "checkpoint.json"



    config = {

        "vehicles": args.vehicles, "target_events": target_events,

        "interval_seconds": args.interval_seconds, "start_utc": start.isoformat(),

        "chunk_size": args.chunk_size, "seed": args.seed, "anomaly_injection_target_rate": 0.15,
        "master_data": str(args.master_data.resolve()),

    }

    manifest = load_json(manifest_path, {"version": 2, "config": config, "chunks": [], "events": 0, "telemetry_bytes": 0, "status": "running"})

    checkpoint = load_json(checkpoint_path, None)

    if args.resume:

        if not checkpoint:

            ap.error("--resume requested but checkpoint.json is missing")

        if manifest.get("config") != config or checkpoint.get("config") != config:

            ap.error("Resume config differs from the original run. Use the same vehicles/target/interval/start/chunk-size/seed.")

        # Recover a chunk committed to checkpoint but not yet listed in manifest.

        if checkpoint["events"] > manifest.get("events", 0):

            idx = checkpoint["chunk_index"]

            tf = telemetry_dir / f"telemetry_{idx:06d}.jsonl"

            af = anomaly_dir / f"anomalies_{idx:06d}.jsonl"

            if not tf.exists() or sha256_file(tf) != checkpoint["last_telemetry_sha256"]:

                ap.error("Checkpoint is ahead of manifest but its telemetry chunk is missing/invalid; refusing unsafe resume.")

            if af.exists() and sha256_file(af) != checkpoint["last_anomaly_sha256"]:

                ap.error("Checkpoint anomaly chunk checksum mismatch; refusing unsafe resume.")

            if len(manifest["chunks"]) < idx:

                manifest["chunks"].append({

                    "index": idx, "events": checkpoint["last_chunk_events"],

                    "telemetry_file": str(tf.relative_to(out)), "anomaly_file": str(af.relative_to(out)),

                    "telemetry_bytes": tf.stat().st_size, "telemetry_sha256": checkpoint["last_telemetry_sha256"],

                    "anomaly_sha256": checkpoint["last_anomaly_sha256"]

                })

            manifest["events"] = checkpoint["events"]

            manifest["telemetry_bytes"] = sum(c["telemetry_bytes"] for c in manifest["chunks"])

            atomic_json(manifest_path, manifest)

        states = checkpoint["states"]

        next_interval = checkpoint["next_interval"]

        chunk_index = checkpoint["chunk_index"]

        completed_events = checkpoint["events"]

        total_bytes = manifest.get("telemetry_bytes", 0)

        print(f"Resuming at interval {next_interval}; committed events={completed_events:,}")

    else:

        existing = list(telemetry_dir.glob("telemetry_*.jsonl"))

        if existing or checkpoint_path.exists() or manifest_path.exists():

            ap.error(f"Output already contains a run: {out}. Choose another --output or use --resume.")

        states = [make_profile(i, args.seed) for i in range(1, args.vehicles + 1)]

        next_interval, chunk_index, completed_events, total_bytes = 0, 0, 0, 0

        checkpoint = {"version": 2, "config": config, "states": states, "next_interval": 0,

                      "chunk_index": 0, "events": 0}

        atomic_json(checkpoint_path, checkpoint)

        atomic_json(manifest_path, manifest)

        trips = [build_trip(s, start, duration_seconds, args.seed) for s in states]

        atomic_json(out / "trips.json", trips)



    validate_master_data(args.master_data.resolve(), states)

    schema = None

    if args.validate_all:

        if not SCHEMA_PATH.exists():

            ap.error(f"Schema not found: {SCHEMA_PATH}")

        schema = load_json(SCHEMA_PATH, {})



    events_per_chunk = args.chunk_size

    intervals_per_chunk = events_per_chunk // args.vehicles

    # Any unreferenced output chunk is an interrupted, uncommitted write; remove it safely.

    committed_names = {Path(c["telemetry_file"]).name for c in manifest["chunks"]}

    for f in telemetry_dir.glob("telemetry_*.jsonl"):

        if f.name not in committed_names:

            f.unlink()

    committed_anomalies = {Path(c["anomaly_file"]).name for c in manifest["chunks"]}

    for f in anomaly_dir.glob("anomalies_*.jsonl"):

        if f.name not in committed_anomalies:

            f.unlink()



    while completed_events < target_events:

        this_intervals = min(intervals_per_chunk, target_intervals - next_interval)

        if this_intervals <= 0:

            break

        idx = chunk_index + 1

        tf = telemetry_dir / f"telemetry_{idx:06d}.jsonl"

        af = anomaly_dir / f"anomalies_{idx:06d}.jsonl"

        tmp_tf, tmp_af = tf.with_suffix(".jsonl.tmp"), af.with_suffix(".jsonl.tmp")

        chunk_events = 0

        with (tmp_tf.open("w", encoding="utf-8", newline="\n", buffering=1024 * 1024) as telemetry,
              tmp_af.open("w", encoding="utf-8", newline="\n", buffering=256 * 1024) as anomaly_file):

            for interval_idx in range(next_interval, next_interval + this_intervals):

                for pstate in states:

                    event, labels = make_event(pstate, interval_idx, start, args.interval_seconds, args.seed, target_intervals)

                    if schema:

                        validate_with_schema(event, schema)

                    telemetry.write(json.dumps(event, separators=(",", ":"), ensure_ascii=False) + "\n")

                    for lab in labels:

                        anomaly_file.write(json.dumps(lab, separators=(",", ":"), ensure_ascii=False) + "\n")

                    chunk_events += 1

            telemetry.flush(); os.fsync(telemetry.fileno())

            anomaly_file.flush(); os.fsync(anomaly_file.fileno())

        os.replace(tmp_tf, tf)

        os.replace(tmp_af, af)

        t_sha, a_sha = sha256_file(tf), sha256_file(af)

        new_events = completed_events + chunk_events

        new_checkpoint = {

            "version": 2, "config": config, "states": states,

            "next_interval": next_interval + this_intervals,

            "chunk_index": idx, "events": new_events,

            "last_chunk_events": chunk_events,

            "last_telemetry_sha256": t_sha, "last_anomaly_sha256": a_sha,

        }

        # Checkpoint first, then manifest. Resume can safely recover this small commit window.

        atomic_json(checkpoint_path, new_checkpoint)

        record = {

            "index": idx, "events": chunk_events, "telemetry_file": str(tf.relative_to(out)),

            "anomaly_file": str(af.relative_to(out)), "telemetry_bytes": tf.stat().st_size,

            "telemetry_sha256": t_sha, "anomaly_sha256": a_sha,

        }

        manifest["chunks"].append(record)

        manifest["events"] = new_events

        manifest["telemetry_bytes"] = sum(c["telemetry_bytes"] for c in manifest["chunks"])

        manifest["status"] = "running"

        atomic_json(manifest_path, manifest)

        completed_events, total_bytes = new_events, manifest["telemetry_bytes"]

        next_interval += this_intervals

        chunk_index = idx

        print(f"Committed chunk {idx}: {chunk_events:,} events; {completed_events:,}/{target_events:,}; telemetry={total_bytes / (1024**3):.3f} GiB")

    manifest["status"] = "completed"

    manifest["completed_at_utc"] = datetime.now(timezone.utc).isoformat()

    manifest["simulated_end_utc"] = (start + timedelta(seconds=max(0, next_interval - 1) * args.interval_seconds)).isoformat()

    manifest["simulated_duration_hours"] = max(0, next_interval - 1) * args.interval_seconds / 3600

    manifest["anomaly_records"] = sum(

        sum(1 for _ in (out / c["anomaly_file"]).open(encoding="utf-8"))

        for c in manifest["chunks"] if (out / c["anomaly_file"]).exists()

    )

    atomic_json(manifest_path, manifest)

    print("\nCompleted.")

    print(f"Events: {completed_events:,}")

    print(f"Vehicles: {args.vehicles:,}")

    print(f"Simulated duration: {manifest['simulated_duration_hours']:.2f} hours")

    print(f"Telemetry JSONL size: {manifest['telemetry_bytes'] / (1024**3):.3f} GiB")

    print(f"Anomaly records: {manifest['anomaly_records']:,}")

    print(f"Output: {out}")

    return 0





if __name__ == "__main__":

    try:

        raise SystemExit(main())

    except KeyboardInterrupt:

        print("\nInterrupted. Last committed chunk and checkpoint are preserved; rerun with --resume.", file=sys.stderr)

        raise SystemExit(130)
