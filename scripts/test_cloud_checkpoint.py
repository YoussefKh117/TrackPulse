import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv(override=True)

connection = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not connection:
    raise RuntimeError(
        "AZURE_STORAGE_CONNECTION_STRING is missing from .env"
    )

container_name = "trackpulse-data"
blob_name = "checkpoints/producer/_cloud_checkpoint_test.json"

test_checkpoint = {
    "checkpoint_type": "connectivity_test",
    "next_interval": 2000,
    "emitted_events": 0,
    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
}

service = BlobServiceClient.from_connection_string(connection)
blob = service.get_blob_client(
    container=container_name,
    blob=blob_name,
)

try:
    # Write a temporary checkpoint to Azure.
    blob.upload_blob(
        json.dumps(test_checkpoint, indent=2),
        overwrite=True,
        content_type="application/json",
    )
    print("PASS: Checkpoint written to Azure Storage.")

    # Read it back and verify its contents.
    downloaded = json.loads(blob.download_blob().readall())

    if downloaded != test_checkpoint:
        raise RuntimeError("Checkpoint verification failed.")

    print("PASS: Checkpoint read back and verified.")
    print(f"Container: {container_name}")
    print(f"Blob path: {blob_name}")

finally:
    # Remove only this temporary test blob.
    try:
        blob.delete_blob()
        print("Temporary test checkpoint removed.")
    except Exception as exc:
        print(f"Could not remove test blob: {exc}")
