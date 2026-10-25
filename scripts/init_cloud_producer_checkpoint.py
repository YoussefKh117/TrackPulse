import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv(override=True)

ROOT = Path(__file__).resolve().parents[1]
HIST_DIR = ROOT / "data" / "historical_azure"
CONTAINER = "trackpulse-data"
BLOB_PATH = "checkpoints/producer/stream_checkpoint.json"


def main():
    connection = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is missing.")

    manifest_path = HIST_DIR / "manifest.json"
    history_path = HIST_DIR / "checkpoint.json"

    if not manifest_path.exists() or not history_path.exists():
        raise FileNotFoundError("Historical manifest/checkpoint not found.")

    with manifest_path.open(encoding="utf-8") as f:
        manifest = json.load(f)

    with history_path.open(encoding="utf-8") as f:
        history = json.load(f)

    if manifest.get("status") != "completed":
        raise RuntimeError("Historical generation is not completed.")

    config = manifest["config"]
    vehicles = int(config["vehicles"])
    target_events = int(config["target_events"])
    expected_interval = target_events // vehicles

    if int(manifest.get("events", -1)) != target_events:
        raise RuntimeError("Historical manifest event count mismatch.")

    if int(history.get("next_interval", -1)) != expected_interval:
        raise RuntimeError("Historical checkpoint is not at the expected end.")

    states = history.get("states")
    if not isinstance(states, list) or len(states) != vehicles:
        raise RuntimeError("Historical vehicle states are missing or invalid.")

    checkpoint = {
        "version": 1,
        "historical_output": str(HIST_DIR.resolve()),
        "historical_events": target_events,
        "next_interval": expected_interval,
        "emitted_events": 0,
        "states": states,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    service = BlobServiceClient.from_connection_string(connection)
    blob = service.get_blob_client(CONTAINER, BLOB_PATH)

    try:
        if blob.exists():
            raise RuntimeError(
                f"Cloud checkpoint already exists: {CONTAINER}/{BLOB_PATH}. "
                "Nothing was overwritten."
            )

        blob.upload_blob(
            json.dumps(checkpoint, separators=(",", ":")),
            overwrite=False,
            content_type="application/json",
        )

        saved = json.loads(blob.download_blob().readall())

        if (
            saved.get("next_interval") != expected_interval
            or saved.get("historical_events") != target_events
            or len(saved.get("states", [])) != vehicles
        ):
            raise RuntimeError("Cloud checkpoint verification failed.")

        print("SUCCESS: Initial producer checkpoint created and verified.")
        print(f"Container: {CONTAINER}")
        print(f"Blob: {BLOB_PATH}")
        print(f"Next interval: {saved['next_interval']}")
        print(f"Historical events: {saved['historical_events']:,}")
        print(f"Vehicle states: {len(saved['states']):,}")
        print("No telemetry events were sent.")

    finally:
        service.close()


if __name__ == "__main__":
    main()
