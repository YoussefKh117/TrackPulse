from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


# Keep routes and IDs aligned with the current telemetry generator.
ROUTES = [
    ("cairo_ring", [
        (30.0444, 31.2357), (30.0131, 31.2089),
        (29.9820, 31.1342), (30.0074, 31.4200),
        (30.0444, 31.2357)
    ]),
    ("new_cairo", [
        (30.0444, 31.2357), (30.0074, 31.4200),
        (30.0276, 31.4913), (30.0131, 31.4518),
        (30.0444, 31.2357)
    ]),
    ("alex_desert", [
        (30.0444, 31.2357), (30.1200, 30.9500),
        (30.8200, 29.6000), (31.2001, 29.9187)
    ]),
    ("ismailia", [
        (30.0444, 31.2357), (30.1500, 31.5000),
        (30.5900, 32.2700), (30.5965, 32.2715)
    ]),
    ("suez", [
        (30.0444, 31.2357), (29.9500, 31.5000),
        (29.9000, 32.5000), (29.9668, 32.5498)
    ]),
    ("fayoum", [
        (30.0444, 31.2357), (29.9500, 31.1500),
        (29.5000, 30.8000), (29.3084, 30.8428)
    ]),
    ("beni_suef", [
        (30.0444, 31.2357), (29.8000, 31.2000),
        (29.0661, 31.0994)
    ]),
    ("assuit", [
        (30.0444, 31.2357), (29.3000, 31.0000),
        (27.1800, 31.1837)
    ]),
    ("port_said", [
        (30.0444, 31.2357), (30.3000, 32.0000),
        (31.2565, 32.2841)
    ]),
    ("north_cairo", [
        (30.0444, 31.2357), (30.1500, 31.2500),
        (30.2500, 31.2000), (30.0444, 31.2357)
    ]),
]


def parse_start(value: str) -> datetime:
    dt = datetime.fromisoformat(
        value.strip().replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        raise argparse.ArgumentTypeError(
            "Start time must include timezone, "
            "e.g. 2026-10-25T00:00:00Z"
        )

    return dt.astimezone(timezone.utc).replace(microsecond=0)


def make_profile(vehicle_num: int, seed: int) -> dict:
    """
    Keep ID-generation rules aligned with make_profile()
    in generate_historical_data_v2.py.
    """
    rng = random.Random(
        seed * 1_000_003 + vehicle_num * 9176
    )

    route_idx = rng.randrange(len(ROUTES))

    vehicle_type = rng.choices(
        ["van", "truck", "refrigerated"],
        weights=[0.22, 0.48, 0.30]
    )[0]

    driver_num = (
        (vehicle_num - 1)
        % max(1, int(vehicle_num * 0.72))
        + 1
    )

    return {
        "vehicle_id": f"VEH-{vehicle_num:05d}",
        "driver_id": f"DRV-{driver_num:05d}",
        "route_id": f"RTE-{route_idx + 1:03d}",
        "route_idx": route_idx,
        "kind": vehicle_type,
    }


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open(
        "w",
        newline="",
        encoding="utf-8-sig"
    ) as file:
        if not rows:
            return

        writer = csv.DictWriter(
            file,
            fieldnames=list(rows[0].keys())
        )

        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate TrackPulse master/reference data."
    )

    parser.add_argument(
        "--vehicles",
        type=int,
        default=5000
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42
    )

    parser.add_argument(
        "--start-utc",
        default="2026-10-25T00:00:00Z"
    )

    parser.add_argument(
        "--duration-hours",
        type=float,
        default=24.0
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/master_data")
    )

    args = parser.parse_args()

    if args.vehicles < 1:
        parser.error("--vehicles must be >= 1")

    if args.duration_hours <= 0:
        parser.error("--duration-hours must be > 0")

    start = parse_start(args.start_utc)

    end = start + timedelta(
        hours=args.duration_hours
    )

    args.output.mkdir(
        parents=True,
        exist_ok=True
    )

    profiles = [
        make_profile(i, args.seed)
        for i in range(1, args.vehicles + 1)
    ]

    # -------------------------------------------------
    # 1. VEHICLES
    # One record per vehicle.
    # -------------------------------------------------

    vehicles = []

    for i, profile in enumerate(profiles, start=1):
        kind = profile["kind"]

        rng = random.Random(args.seed + i)

        vehicles.append({
            "vehicle_id": profile["vehicle_id"],
            "vehicle_type": kind,

            "model": {
                "van": "Transit Van",
                "truck": "Heavy Duty Truck",
                "refrigerated": "Refrigerated Truck",
            }[kind],

            "manufacture_year": rng.randint(2018, 2025),

            "fuel_type": "diesel",

            "capacity_kg": {
                "van": 1500,
                "truck": 12000,
                "refrigerated": 8000,
            }[kind],

            "status": "active",
        })

    # -------------------------------------------------
    # 2. DRIVERS
    # One record per unique driver ID.
    # -------------------------------------------------

    driver_ids = sorted({
        profile["driver_id"]
        for profile in profiles
    })

    drivers = []

    for i, driver_id in enumerate(driver_ids, start=1):
        drivers.append({
            "driver_id": driver_id,
            "driver_label": f"Driver {i:05d}",

            "license_category": (
                "professional_heavy"
                if i % 3
                else "professional_light"
            ),

            "employment_status": "active",
        })

    # -------------------------------------------------
    # 3. ROUTES
    # Route IDs match the telemetry generator.
    # -------------------------------------------------

    routes = []

    for i, (name, points) in enumerate(ROUTES, start=1):
        routes.append({
            "route_id": f"RTE-{i:03d}",
            "route_name": name,

            "origin_name": "Cairo",

            "destination_name": (
                name.replace("_", " ").title()
            ),

            "origin_latitude": points[0][0],
            "origin_longitude": points[0][1],

            "destination_latitude": points[-1][0],
            "destination_longitude": points[-1][1],

            "planned_distance_km": "",
            "planned_duration_min": "",

            "deviation_threshold_m": 1500,

            "route_note": (
                "Synthetic corridor/waypoints; "
                "not a routable road-network route"
            ),
        })

    # -------------------------------------------------
    # 4. SHIPMENTS
    # One shipment per vehicle/trip in this version.
    # -------------------------------------------------

    shipments = []

    for i, profile in enumerate(profiles, start=1):
        refrigerated = (
            profile["kind"] == "refrigerated"
        )

        shipments.append({
            "shipment_id": f"SHP-{i:05d}",

            "cargo_type": (
                "temperature_controlled_food"
                if refrigerated
                else "general_goods"
            ),

            "required_temp_min_c": (
                -5 if refrigerated else ""
            ),

            "required_temp_max_c": (
                5 if refrigerated else ""
            ),

            "delivery_status": "planned",
        })

    # -------------------------------------------------
    # 5. TRIP PLANS
    # One planned trip per vehicle.
    # IDs match build_trip() in the current generator.
    # -------------------------------------------------

    trip_plans = []

    for i, profile in enumerate(profiles, start=1):
        route_name = ROUTES[profile["route_idx"]][0]

        trip_plans.append({
            "trip_id": f"TRIP-{i:05d}-001",

            "vehicle_id": profile["vehicle_id"],
            "driver_id": profile["driver_id"],

            "shipment_id": f"SHP-{i:05d}",
            "route_id": profile["route_id"],

            "planned_start": (
                start.isoformat().replace("+00:00", "Z")
            ),

            "planned_end": (
                end.isoformat().replace("+00:00", "Z")
            ),

            "origin": "Cairo",

            "destination": (
                route_name.replace("_", " ").title()
            ),

            "trip_status": "planned",
        })

    # -------------------------------------------------
    # WRITE OUTPUT FILES
    # -------------------------------------------------

    write_csv(
        args.output / "vehicles.csv",
        vehicles
    )

    write_csv(
        args.output / "drivers.csv",
        drivers
    )

    write_csv(
        args.output / "routes.csv",
        routes
    )

    write_csv(
        args.output / "shipments.csv",
        shipments
    )

    write_csv(
        args.output / "trip_plans.csv",
        trip_plans
    )

    print("\nMaster data generated successfully.")
    print(f"Output folder: {args.output.resolve()}")

    print(f"Vehicles: {len(vehicles):,}")
    print(f"Unique drivers: {len(drivers):,}")
    print(f"Routes: {len(routes):,}")
    print(f"Shipments: {len(shipments):,}")
    print(f"Trip plans: {len(trip_plans):,}")

    print(
        "\nNote: IDs follow the conventions used by "
        "the current historical generator."
    )

    print(
        "The historical generator must still read or validate "
        "these CSV files to make them the source of truth."
    )


if __name__ == "__main__":
    main()