import json
from collections import Counter
from datetime import datetime
from pathlib import Path


# Project paths
ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "generated"


def load_json(path):
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def load_jsonl(path):
    records = []

    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON at line {line_number}: {error}"
                ) from error

    return records


def parse_timestamp(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


def main():
    # Load generated data
    events = load_jsonl(
        DATA / "telemetry_events.jsonl"
    )
    trips = load_json(DATA / "trips.json")
    breaches = load_json(
        DATA / "cold_chain_breaches.json"
    )

    # Basic counts
    events_per_vehicle = Counter(
        event["vehicle_id"] for event in events
    )

    trip_ids = {
        trip["trip_id"] for trip in trips
    }

    vehicle_ids = {
        trip["vehicle_id"] for trip in trips
    }

    print(f"Telemetry events: {len(events)}")
    print(f"Trips: {len(trips)}")
    print(f"Cold-chain breaches: {len(breaches)}")
    print(
        "Unique vehicles in telemetry: "
        f"{len(events_per_vehicle)}"
    )

    print("\nEvents per vehicle:")
    for vehicle_id, count in sorted(
        events_per_vehicle.items()
    ):
        print(f"  {vehicle_id}: {count}")

    # Check expected counts
    assert len(events) == 600, (
        f"Expected 600 events, found {len(events)}"
    )

    assert len(trips) == 10, (
        f"Expected 10 trips, found {len(trips)}"
    )

    assert len(events_per_vehicle) == 10, (
        f"Expected 10 vehicles, "
        f"found {len(events_per_vehicle)}"
    )

    assert all(
        count == 60
        for count in events_per_vehicle.values()
    ), "Each vehicle must have exactly 60 events"

    print("\nPASS: Expected record counts")

    # Check relationships between telemetry and trips
    assert all(
        event["trip_id"] in trip_ids
        for event in events
    ), "Some telemetry events reference unknown trips"

    assert all(
        event["vehicle_id"] in vehicle_ids
        for event in events
    ), "Some telemetry events reference unknown vehicles"

    print("PASS: Telemetry-to-trip relationships")

    # Check timestamps are chronological per vehicle
    timestamps_by_vehicle = {}

    for event in events:
        vehicle_id = event["vehicle_id"]
        event_time = parse_timestamp(
            event["event_timestamp"]
        )

        timestamps_by_vehicle.setdefault(
            vehicle_id, []
        ).append(event_time)

    for vehicle_id, timestamps in (
        timestamps_by_vehicle.items()
    ):
        assert timestamps == sorted(timestamps), (
            f"Events for {vehicle_id} "
            "are not chronological"
        )

    print("PASS: Events are chronological per vehicle")

    # Check event IDs are unique
    event_ids = [
        event["event_id"] for event in events
    ]

    assert len(event_ids) == len(set(event_ids)), (
        "Duplicate event_id values found"
    )

    print("PASS: Event IDs are unique")

    # Check cold-chain breach references
    shipment_ids = {
        trip["shipment_id"] for trip in trips
    }

    for breach in breaches:
        assert breach["trip_id"] in trip_ids, (
            f"Unknown trip in breach: {breach['trip_id']}"
        )

        assert breach["vehicle_id"] in vehicle_ids, (
            "Unknown vehicle in breach: "
            f"{breach['vehicle_id']}"
        )

        assert breach["shipment_id"] in shipment_ids, (
            "Unknown shipment in breach: "
            f"{breach['shipment_id']}"
        )

        start_time = parse_timestamp(
            breach["start_timestamp"]
        )

        if breach["end_timestamp"] is not None:
            end_time = parse_timestamp(
                breach["end_timestamp"]
            )

            assert end_time >= start_time, (
                "Breach end_timestamp is before start_timestamp"
            )

    print("PASS: Cold-chain breach relationships")

    print("\nAll generated-data checks passed.")


if __name__ == "__main__":
    main()
