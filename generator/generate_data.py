import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


# ============================================================
# 1. Configuration
# ============================================================

NUM_VEHICLES = 10
DURATION_MINUTES = 10
INTERVAL_SECONDS = 10
RANDOM_SEED = 42

# The cargo temperature limit used by our demo scenario.
CARGO_TEMP_MIN_C = 2.0
CARGO_TEMP_MAX_C = 8.0

ROOT = Path(__file__).resolve().parent.parent
CONTRACTS = ROOT / "contracts"
OUTPUT = ROOT / "data" / "generated"

random.seed(RANDOM_SEED)


# ============================================================
# 2. Utility functions
# ============================================================

def timestamp(value):
    """Return a UTC timestamp in ISO 8601 format."""
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def load_validator(filename):
    """Load and check a JSON Schema."""
    with (CONTRACTS / filename).open(
        "r", encoding="utf-8-sig"
    ) as file:
        schema = json.load(file)

    Draft202012Validator.check_schema(schema)

    return Draft202012Validator(
        schema,
        format_checker=FormatChecker()
    )


def validate(records, validator, label):
    """Validate every record against its schema."""
    errors = []

    for index, record in enumerate(records):
        for error in validator.iter_errors(record):
            errors.append(
                f"{label}[{index}]: {error.message}"
            )

    if errors:
        raise ValueError("\n".join(errors[:20]))

    print(f"PASS: {label} schema ({len(records)} records)")


def save_json(path, records):
    with path.open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2)


def clamp(value, lower, upper):
    return max(lower, min(value, upper))


# ============================================================
# 3. Normal behavior
# ============================================================

def normal_speed(previous_speed):
    """
    Generate temporally correlated vehicle speed.

    Speed changes gradually instead of being independently
    randomized at every event.
    """
    target_speed = random.uniform(35.0, 65.0)

    speed = previous_speed + (
        target_speed - previous_speed
    ) * 0.35

    speed += random.uniform(-2.0, 2.0)

    return round(clamp(speed, 0.0, 85.0), 2)


def normal_engine_temp(previous_temp, speed, rpm):
    """
    Approximate engine thermal behavior.

    Higher RPM and speed increase the target temperature.
    The engine temperature moves gradually toward that target.
    """
    target = (
        82.0
        + speed * 0.10
        + max(rpm - 1200, 0) * 0.004
    )

    target = clamp(target, 80.0, 102.0)

    new_temp = previous_temp + (
        target - previous_temp
    ) * 0.18

    new_temp += random.uniform(-0.25, 0.25)

    return round(clamp(new_temp, 75.0, 105.0), 2)


def normal_cargo_temp(previous_temp):
    """
    Simulate refrigerated cargo temperature with small,
    gradual variations under normal operating conditions.
    """
    target = 4.5

    new_temp = previous_temp + (
        target - previous_temp
    ) * 0.12

    new_temp += random.uniform(-0.12, 0.12)

    return round(clamp(new_temp, 3.0, 6.0), 2)


def normal_rpm(speed):
    """Approximate RPM according to vehicle speed."""
    base = 900 + speed * 24
    return int(clamp(
        base + random.randint(-100, 100),
        900,
        2800
    ))


def normal_fuel_rate(speed, rpm):
    """
    Approximate fuel consumption.

    Fuel consumption increases with vehicle speed and RPM.
    """
    rate = (
        2.5
        + speed * 0.065
        + max(rpm - 1000, 0) * 0.0015
    )

    rate += random.uniform(-0.3, 0.3)

    return round(clamp(rate, 1.5, 12.0), 2)


def route_position(progress, vehicle_number):
    """
    Create a simple simulated route from Cairo.

    This is a straight-line demo route, not a real road network.
    Each vehicle receives a small parallel starting offset.
    """
    start_lat = 30.0444 + vehicle_number * 0.00015
    start_lon = 31.2357 + vehicle_number * 0.00015

    end_lat = start_lat + 0.0556
    end_lon = start_lon + 0.1643

    lat = start_lat + (end_lat - start_lat) * progress
    lon = start_lon + (end_lon - start_lon) * progress

    return lat, lon


# ============================================================
# 4. Controlled anomaly scenarios
# ============================================================

def deviation_offset(seconds):
    """
    Vehicle 003 deviates from its planned route.

    The offset ramps in and out instead of teleporting the
    vehicle several kilometers away in one event.
    """
    start = 240
    peak = 280
    end = 360

    if seconds < start or seconds >= end:
        return 0.0

    if seconds <= peak:
        return (seconds - start) / (peak - start)

    return (end - seconds) / (end - peak)


def inject_idling(vehicle_number, seconds):
    """Vehicle 002 idles between minute 2 and minute 5."""
    return (
        vehicle_number == 2
        and 120 <= seconds < 300
    )


def inject_cold_chain_breach(
    vehicle_number,
    seconds,
    normal_temp
):
    """
    Vehicle 001 experiences a controlled cold-chain breach.

    Temperature rises above the 8 C limit and later recovers.
    The telemetry schema remains unchanged.
    """
    if vehicle_number != 1:
        return normal_temp

    if 180 <= seconds < 300:
        # Simulated cooling failure:
        # temperature remains above the allowed maximum.
        return round(
            9.5 + 0.8 * math.sin(
                (seconds - 180) / 120 * math.pi
            ),
            2
        )

    return normal_temp


# ============================================================
# 5. Data generation
# ============================================================

def generate_data():
    start = datetime.now(timezone.utc).replace(
        microsecond=0
    )

    number_of_intervals = (
        DURATION_MINUTES * 60 // INTERVAL_SECONDS
    )

    vehicles = {}

    trips = []
    events = []
    breaches = []
    scenarios = []

    # Create vehicle state and trip records.
    for n in range(1, NUM_VEHICLES + 1):
        vehicle_id = f"veh-{n:03d}"
        trip_id = f"trip-{n:03d}"
        shipment_id = f"ship-{n:03d}"
        driver_id = f"driver-{n:03d}"
        route_id = f"route-{n:03d}"

        vehicles[vehicle_id] = {
            "vehicle_number": n,
            "trip_id": trip_id,
            "shipment_id": shipment_id,
            "driver_id": driver_id,
            "route_id": route_id,
            "previous_speed": 45.0,
            "previous_engine_temp": 88.0,
            "previous_cargo_temp": 4.5,
            "odometer_km": 12000.0 + n * 125,
            "fuel_used_liters": 0.0,
        }

        trips.append({
            "trip_id": trip_id,
            "vehicle_id": vehicle_id,
            "driver_id": driver_id,
            "shipment_id": shipment_id,
            "route_id": route_id,
            "planned_start": timestamp(start),
            "planned_end": timestamp(
                start + timedelta(minutes=45)
            ),
            "actual_start": timestamp(start),
            "actual_end": None,
            "origin": "Cairo Distribution Center",
            "destination": f"Delivery Zone {n:02d}",
            "trip_status": "in_progress",
            "distance_km": None,
            "fuel_used_liters": None,
        })

    # Define scenarios separately from telemetry records.
    scenarios.extend([
        {
            "scenario_id": "scenario-cold-001",
            "scenario_type": "cold_chain_breach",
            "vehicle_id": "veh-001",
            "trip_id": "trip-001",
            "shipment_id": "ship-001",
            "start_timestamp": timestamp(
                start + timedelta(seconds=180)
            ),
            "end_timestamp": timestamp(
                start + timedelta(seconds=300)
            ),
            "expected_condition": "cargo_temp_c > 8.0",
            "severity": "high",
        },
        {
            "scenario_id": "scenario-idle-001",
            "scenario_type": "excessive_idling",
            "vehicle_id": "veh-002",
            "trip_id": "trip-002",
            "start_timestamp": timestamp(
                start + timedelta(seconds=120)
            ),
            "end_timestamp": timestamp(
                start + timedelta(seconds=300)
            ),
            "expected_condition": (
                "speed_kph == 0 and ignition_on == true"
            ),
            "severity": "medium",
        },
        {
            "scenario_id": "scenario-route-001",
            "scenario_type": "route_deviation",
            "vehicle_id": "veh-003",
            "trip_id": "trip-003",
            "start_timestamp": timestamp(
                start + timedelta(seconds=240)
            ),
            "end_timestamp": timestamp(
                start + timedelta(seconds=360)
            ),
            "expected_condition": (
                "GPS offset from simulated planned route"
            ),
            "severity": "high",
        },
    ])

    # Generate events in timestamp order across all vehicles.
    for interval_index in range(number_of_intervals):
        seconds = interval_index * INTERVAL_SECONDS
        event_time = start + timedelta(seconds=seconds)

        progress = seconds / (
            DURATION_MINUTES * 60
        )

        for vehicle_id, state in vehicles.items():
            n = state["vehicle_number"]

            idling = inject_idling(n, seconds)

            if idling:
                speed = 0.0
                rpm = 850
                fuel_rate = 1.2
            else:
                speed = normal_speed(
                    state["previous_speed"]
                )
                rpm = normal_rpm(speed)
                fuel_rate = normal_fuel_rate(speed, rpm)

            engine_temp = normal_engine_temp(
                state["previous_engine_temp"],
                speed,
                rpm
            )

            cargo_temp = normal_cargo_temp(
                state["previous_cargo_temp"]
            )

            cargo_temp = inject_cold_chain_breach(
                n, seconds, cargo_temp
            )

            lat, lon = route_position(progress, n)

            # Controlled GPS deviation: up to about 400 m.
            offset = deviation_offset(seconds)

            if n == 3:
                lat += 0.003 * offset
                lon += 0.003 * offset

            # Integrate distance using the current speed and
            # elapsed time. Do not multiply speed by total time.
            distance_increment = (
                speed * INTERVAL_SECONDS / 3600
            )

            state["odometer_km"] += distance_increment

            state["fuel_used_liters"] += (
                fuel_rate * INTERVAL_SECONDS / 3600
            )

            event = {
                "event_id": (
                    f"evt-{n:03d}-{interval_index:04d}"
                ),
                "event_timestamp": timestamp(event_time),
                "vehicle_id": vehicle_id,
                "trip_id": state["trip_id"],
                "shipment_id": state["shipment_id"],
                "driver_id": state["driver_id"],
                "route_id": state["route_id"],
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "speed_kph": round(speed, 2),
                "odometer_km": round(
                    state["odometer_km"], 3
                ),
                "rpm": rpm,
                "fuel_rate_lph": fuel_rate,
                "engine_temp_c": engine_temp,
                "cargo_temp_c": cargo_temp,
                "ignition_on": True,
            }

            events.append(event)

            state["previous_speed"] = speed
            state["previous_engine_temp"] = engine_temp
            state["previous_cargo_temp"] = cargo_temp

    # Build a breach record from the actual generated telemetry.
    cold_events = [
        event for event in events
        if event["vehicle_id"] == "veh-001"
        and 180 <= (
            parse_event_time(event) - start
        ).total_seconds() < 300
    ]

    cold_temps = [
        event["cargo_temp_c"] for event in cold_events
    ]

    breaches.append({
        "breach_id": "breach-001",
        "shipment_id": "ship-001",
        "trip_id": "trip-001",
        "vehicle_id": "veh-001",
        "start_timestamp": timestamp(
            start + timedelta(seconds=180)
        ),
        "end_timestamp": timestamp(
            start + timedelta(seconds=300)
        ),
        "min_cargo_temp_c": min(cold_temps),
        "max_cargo_temp_c": max(cold_temps),
        "allowed_min_temp_c": CARGO_TEMP_MIN_C,
        "allowed_max_temp_c": CARGO_TEMP_MAX_C,
        "duration_minutes": 2.0,
        "severity": "high",
        "resolved_flag": True,
    })

    return trips, events, breaches, scenarios


def parse_event_time(event):
    return datetime.fromisoformat(
        event["event_timestamp"].replace(
            "Z", "+00:00"
        )
    )


# ============================================================
# 6. Quality checks
# ============================================================

def run_quality_checks(
    trips,
    events,
    breaches,
    scenarios
):
    assert len(trips) == NUM_VEHICLES
    assert len(events) == (
        NUM_VEHICLES * DURATION_MINUTES * 60
        // INTERVAL_SECONDS
    )

    print("PASS: Expected record counts")

    vehicle_ids = {
        trip["vehicle_id"] for trip in trips
    }
    trip_ids = {trip["trip_id"] for trip in trips}
    shipment_ids = {
        trip["shipment_id"] for trip in trips
    }

    assert all(
        event["vehicle_id"] in vehicle_ids
        and event["trip_id"] in trip_ids
        for event in events
    )

    assert all(
        breach["vehicle_id"] in vehicle_ids
        and breach["trip_id"] in trip_ids
        and breach["shipment_id"] in shipment_ids
        for breach in breaches
    )

    print("PASS: Record relationships")

    event_ids = [
        event["event_id"] for event in events
    ]

    assert len(event_ids) == len(set(event_ids))
    print("PASS: Unique event IDs")

    # Global chronological order.
    event_times = [
        parse_event_time(event) for event in events
    ]

    assert event_times == sorted(event_times)
    print("PASS: Global chronological order")

    # Odometer must never decrease for each vehicle.
    previous_odometer = {}

    for event in events:
        vehicle_id = event["vehicle_id"]
        current = event["odometer_km"]

        if vehicle_id in previous_odometer:
            assert current >= previous_odometer[vehicle_id], (
                f"Odometer decreased for {vehicle_id}"
            )

        previous_odometer[vehicle_id] = current

    print("PASS: Odometer is non-decreasing")

    # The injected cold-chain breach must be above 8 C.
    breach_events = [
        event for event in events
        if event["vehicle_id"] == "veh-001"
        and 180 <= (
            parse_event_time(event) - event_times[0]
        ).total_seconds() < 300
    ]

    assert breach_events
    assert all(
        event["cargo_temp_c"] > CARGO_TEMP_MAX_C
        for event in breach_events
    )

    print("PASS: Cold-chain anomaly injection")

    # Idling must have zero speed and low RPM.
    idle_events = [
        event for event in events
        if event["vehicle_id"] == "veh-002"
        and 120 <= (
            parse_event_time(event) - event_times[0]
        ).total_seconds() < 300
    ]

    assert idle_events
    assert all(
        event["speed_kph"] == 0
        and event["rpm"] == 850
        for event in idle_events
    )

    print("PASS: Idling anomaly injection")

    assert len(scenarios) == 3
    print("PASS: Scenario ground truth")


# ============================================================
# 7. Main
# ============================================================

def main():
    trips, events, breaches, scenarios = generate_data()

    validate(
        events,
        load_validator("telemetry_event.schema.json"),
        "Telemetry"
    )
    validate(
        trips,
        load_validator("trip.schema.json"),
        "Trips"
    )
    validate(
        breaches,
        load_validator("cold_chain_breach.schema.json"),
        "Cold-chain breaches"
    )

    run_quality_checks(
        trips, events, breaches, scenarios
    )

    OUTPUT.mkdir(parents=True, exist_ok=True)

    save_json(OUTPUT / "trips.json", trips)
    save_json(
        OUTPUT / "cold_chain_breaches.json",
        breaches
    )
    save_json(
        OUTPUT / "anomaly_scenarios.json",
        scenarios
    )

    with (
        OUTPUT / "telemetry_events.jsonl"
    ).open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event) + "\n")

    print("\nGeneration completed successfully.")
    print(f"Vehicles: {NUM_VEHICLES}")
    print(f"Trips: {len(trips)}")
    print(f"Telemetry events: {len(events)}")
    print(f"Cold-chain breaches: {len(breaches)}")
    print(f"Ground-truth scenarios: {len(scenarios)}")
    print(f"Output folder: {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
