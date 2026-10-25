import json
import os
from pathlib import Path

from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# Load local configuration
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "telemetry-events")

DATA_FILE = PROJECT_ROOT / "data" / "generated" / "telemetry_events.jsonl"
MAX_EVENTS = 10


def main():
    if not CONNECTION_STRING:
        raise ValueError("EVENT_HUB_CONNECTION_STRING is missing from .env")

    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Generated data file not found: {DATA_FILE}")

    # Read only the first 10 existing events
    events = []
    with DATA_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                events.append(json.loads(line))
                if len(events) >= MAX_EVENTS:
                    break

    if not events:
        raise ValueError("No events found in the JSONL file")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME,
    )

    sent_count = 0

    try:
        batch = producer.create_batch()

        for event in events:
            event_data = EventData(
                json.dumps(event, separators=(",", ":"), ensure_ascii=False)
            )

            try:
                batch.add(event_data)
            except ValueError:
                # Send the current batch, then start a new one
                if len(batch) == 0:
                    raise ValueError(
                        "A single event exceeds the Event Hubs batch size limit"
                    )

                producer.send_batch(batch)
                sent_count += len(batch)
                batch = producer.create_batch()
                batch.add(event_data)

        if len(batch) > 0:
            producer.send_batch(batch)
            sent_count += len(batch)

        print(f"Successfully sent {sent_count} events to Event Hubs.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
