import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

load_dotenv(override=False)

event = {
    "event_id": "connectivity-test-20261025-01",
    "event_type": "connectivity_test",
    "event_time": datetime.now(timezone.utc).isoformat(),
}

producer = EventHubProducerClient.from_connection_string(
    os.environ["EVENT_HUB_CONNECTION_STR"],
    eventhub_name=os.environ["EVENT_HUB_NAME"],
    logging_enable=False,
)

try:
    batch = producer.create_batch()
    batch.add(EventData(json.dumps(event)))
    producer.send_batch(batch)
    print("SUCCESS: Event Hubs acknowledged the test batch.")
    print("Test event ID:", event["event_id"])
finally:
    producer.close()
