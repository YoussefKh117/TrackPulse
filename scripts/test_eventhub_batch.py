import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

load_dotenv(override=False)

producer = EventHubProducerClient.from_connection_string(
    os.environ["EVENT_HUB_CONNECTION_STR"],
    eventhub_name=os.environ["EVENT_HUB_NAME"],
)

try:
    batch = producer.create_batch()

    for i in range(10):
        event = {
            "event_id": f"batch-test-{i}-{datetime.now(timezone.utc).timestamp()}",
            "event_type": "batch_test",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "value": i,
        }
        batch.add(EventData(json.dumps(event)))

    print(f"Sending {len(batch)} events in one batch...")
    producer.send_batch(batch, timeout=30)
    print("SUCCESS: 10-event batch acknowledged by Event Hubs.")
finally:
    producer.close()
