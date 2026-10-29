import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from azure.eventhub import EventData, EventHubProducerClient, TransportType

# Load the project's existing .env explicitly.
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=False)

connection_string = (
    os.getenv("EVENT_HUB_CONNECTION_STR")
    or os.getenv("EVENT_HUB_CONNECTION_STRING")
)
eventhub_name = os.getenv("EVENT_HUB_NAME")

if not connection_string or not eventhub_name:
    raise SystemExit(
        "Missing Event Hubs settings. Check EVENT_HUB_CONNECTION_STR/"
        "EVENT_HUB_CONNECTION_STRING and EVENT_HUB_NAME in .env."
    )

test_id = "fastpath-alerts-20261028-03"
now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

events = [
    {
        "event_id": f"TEST-{test_id}-engine",
        "event_timestamp": now,
        "vehicle_id": "VEH-TEST-00001",
        "latitude": 30.0444,
        "longitude": 31.2357,
        "rpm": 1800,
        "fuel_rate_lph": 2.5,
        "engine_temp_c": 112.0,
        "cargo_temp_c": 1.0,
        "test_mode": True,
        "test_id": test_id,
    },
    {
        "event_id": f"TEST-{test_id}-cargo",
        "event_timestamp": now,
        "vehicle_id": "VEH-TEST-00002",
        "latitude": 30.0444,
        "longitude": 31.2357,
        "rpm": 1500,
        "fuel_rate_lph": 1.8,
        "engine_temp_c": 85.0,
        "cargo_temp_c": 5.0,
        "test_mode": True,
        "test_id": test_id,
    },
    {
        "event_id": f"TEST-{test_id}-normal",
        "event_timestamp": now,
        "vehicle_id": "VEH-TEST-00003",
        "latitude": 30.0444,
        "longitude": 31.2357,
        "rpm": 1200,
        "fuel_rate_lph": 1.2,
        "engine_temp_c": 85.0,
        "cargo_temp_c": 1.0,
        "test_mode": True,
        "test_id": test_id,
    },
]

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_string,
    eventhub_name=eventhub_name,
    transport_type=TransportType.AmqpOverWebsocket,
)

try:
    batch = producer.create_batch()
    for event in events:
        batch.add(EventData(json.dumps(event)))
    producer.send_batch(batch)
    print(f"Successfully sent {len(events)} test events.")
    print(f"Test ID: {test_id}")
    print(f"Event Hub: {eventhub_name}")
finally:
    producer.close()
