# # from __future__ import annotations

# # import argparse
# # import importlib.util
# # import json
# # import os
# # import sys
# # import time
# # from datetime import datetime, timezone
# # from pathlib import Path
# # from typing import Any

# # from azure.eventhub import EventData, EventHubProducerClient


# # def load_generator(path: Path):
# #     spec = importlib.util.spec_from_file_location("trackpulse_historical_generator", path)
# #     if spec is None or spec.loader is None:
# #         raise RuntimeError(f"Cannot load generator module: {path}")
# #     module = importlib.util.module_from_spec(spec)
# #     sys.modules[spec.name] = module
# #     spec.loader.exec_module(module)
# #     return module


# # def read_json(path: Path) -> dict[str, Any]:
# #     with path.open("r", encoding="utf-8") as f:
# #         return json.load(f)


# # def atomic_json(path: Path, obj: dict[str, Any]) -> None:
# #     temp = path.with_suffix(path.suffix + ".tmp")
# #     with temp.open("w", encoding="utf-8") as f:
# #         json.dump(obj, f, ensure_ascii=False, indent=2)
# #         f.flush()
# #         os.fsync(f.fileno())
# #     temp.replace(path)


# # def main() -> int:
# #     parser = argparse.ArgumentParser(
# #         description="Continue TrackPulse telemetry after historical data and send new events to Azure Event Hubs."
# #     )
# #     parser.add_argument("--historical-output", type=Path, required=True)
# #     parser.add_argument("--generator", type=Path, required=True,
# #                         help="Path to generate_historical_data_trackpulse.py")
# #     parser.add_argument("--sleep-seconds", type=float, default=1.0,
# #                         help="Wall-clock pause after each fleet batch; 0 means send as fast as possible.")
# #     parser.add_argument("--batch-size", type=int, default=5000,
# #                         help="Events per send batch. Use 5000 to send one full fleet interval at a time.")
# #     parser.add_argument("--max-events", type=int, default=0,
# #                         help="Stop after this many new events; 0 means run until interrupted.")
# #     parser.add_argument("--checkpoint", type=Path, default=None)
# #     args = parser.parse_args()

# #     if args.sleep_seconds < 0 or args.batch_size < 1 or args.max_events < 0:
# #         parser.error("sleep-seconds and max-events must be >= 0; batch-size must be >= 1")

# #     connection = os.getenv("EVENT_HUB_CONNECTION_STR")
# #     hub_name = os.getenv("EVENT_HUB_NAME")
# #     if not connection or not hub_name:
# #         parser.error("Set EVENT_HUB_CONNECTION_STR and EVENT_HUB_NAME environment variables first.")

# #     hist_dir = args.historical_output.resolve()
# #     manifest_path = hist_dir / "manifest.json"
# #     historical_checkpoint_path = hist_dir / "checkpoint.json"
# #     if not manifest_path.exists() or not historical_checkpoint_path.exists():
# #         parser.error("Historical manifest/checkpoint not found. Generate historical data first.")

# #     manifest = read_json(manifest_path)
# #     checkpoint = read_json(historical_checkpoint_path)
# #     if manifest.get("status") != "completed":
# #         parser.error(f"Historical generation is not completed (status={manifest.get('status')!r}).")
# #     config = manifest.get("config", {})
# #     vehicles = int(config.get("vehicles", 0))
# #     target_events = int(config.get("target_events", 0))
# #     interval_seconds = int(config.get("interval_seconds", 0))
# #     if vehicles < 1 or interval_seconds < 1:
# #         parser.error("Historical manifest has invalid vehicles/interval_seconds.")
# #     if int(manifest.get("events", -1)) != target_events:
# #         parser.error("Manifest event count does not equal configured target_events.")
# #     next_interval = int(checkpoint.get("next_interval", -1))
# #     if next_interval != target_events // vehicles:
# #         parser.error(
# #             f"Checkpoint is not at the end of history: next_interval={next_interval}, "
# #             f"expected={target_events // vehicles}."
# #         )
# #     states = checkpoint.get("states")
# #     if not isinstance(states, list) or len(states) != vehicles:
# #         parser.error("Historical checkpoint does not contain all vehicle states.")

# #     start_text = config.get("start_utc")
# #     if not start_text:
# #         parser.error("Historical manifest does not contain start_utc.")
# #     start = datetime.fromisoformat(start_text.replace("Z", "+00:00")).astimezone(timezone.utc)
# #     seed = int(config.get("seed", 42))
# #     total_intervals_est = max(next_interval + 100000, 1)
# #     generator = load_generator(args.generator.resolve())

# #     checkpoint_path = (args.checkpoint or (hist_dir / "stream_checkpoint.json")).resolve()
# #     stream_state = None
# #     if checkpoint_path.exists():
# #         stream_state = read_json(checkpoint_path)
# #         if stream_state.get("historical_output") != str(hist_dir):
# #             parser.error("Streaming checkpoint belongs to a different historical output.")
# #         if stream_state.get("historical_events") != target_events:
# #             parser.error("Streaming checkpoint historical event count does not match.")
# #         states = stream_state["states"]
# #         next_interval = int(stream_state["next_interval"])
# #         emitted_events = int(stream_state.get("emitted_events", 0))
# #     else:
# #         emitted_events = 0
# #         atomic_json(checkpoint_path, {
# #             "version": 1,
# #             "historical_output": str(hist_dir),
# #             "historical_events": target_events,
# #             "next_interval": next_interval,
# #             "emitted_events": 0,
# #             "states": states,
# #             "created_at_utc": datetime.now(timezone.utc).isoformat(),
# #         })

# #     producer = EventHubProducerClient.from_connection_string(
# #         conn_str=connection,
# #         eventhub_name=hub_name,
# #     )
# #     print(f"Historical events: {target_events:,}")
# #     print(f"Continuing from interval {next_interval}; first new event index is {next_interval}.")
# #     print(f"Streaming checkpoint: {checkpoint_path}")
# #     print("Press Ctrl+C to stop. Restarting will continue from stream_checkpoint.json.")

# #     try:
# #         with producer:
# #             while True:
# #                 if args.max_events and emitted_events >= args.max_events:
# #                     break

# #                 # Snapshot the interval's starting state. The durable checkpoint advances
# #                 # only after every vehicle event for this interval has been acknowledged.
# #                 interval_state_before = json.loads(json.dumps(states))
# #                 interval_events_sent = 0
# #                 batch = producer.create_batch()

# #                 for p in states:
# #                     if args.max_events and emitted_events + interval_events_sent >= args.max_events:
# #                         break
# #                     event, _labels = generator.make_event(
# #                         p, next_interval, start, interval_seconds, seed, total_intervals_est
# #                     )
# #                     payload = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
# #                     try:
# #                         batch.add(EventData(payload))
# #                     except ValueError:
# #                         if len(batch) == 0:
# #                             raise RuntimeError(
# #                                 "A single telemetry event is larger than the Event Hubs batch limit."
# #                             )
# #                         producer.send_batch(batch)
# #                         interval_events_sent += len(batch)
# #                         batch = producer.create_batch()
# #                         batch.add(EventData(payload))

# #                 if len(batch):
# #                     producer.send_batch(batch)
# #                     interval_events_sent += len(batch)

# #                 emitted_events += interval_events_sent
# #                 if interval_events_sent == vehicles:
# #                     next_interval += 1
# #                     # Save updated state only after all events for the interval were sent.
# #                     atomic_json(checkpoint_path, {
# #                         "version": 1, "historical_output": str(hist_dir),
# #                         "historical_events": target_events, "next_interval": next_interval,
# #                         "emitted_events": emitted_events, "states": states,
# #                         "updated_at_utc": datetime.now(timezone.utc).isoformat(),
# #                     })
# #                 else:
# #                     # A max-events cutoff in the middle of a fleet interval is intentionally
# #                     # not checkpointed as complete; restore state to the interval boundary.
# #                     states = interval_state_before
# #                     atomic_json(checkpoint_path, {
# #                         "version": 1, "historical_output": str(hist_dir),
# #                         "historical_events": target_events, "next_interval": next_interval,
# #                         "emitted_events": emitted_events, "states": states,
# #                         "updated_at_utc": datetime.now(timezone.utc).isoformat(),
# #                         "warning": "Partial fleet interval may be resent on restart; deduplicate by event_id.",
# #                     })
# #                 if args.max_events and emitted_events >= args.max_events:
# #                     break
# #                 if args.sleep_seconds:
# #                     time.sleep(args.sleep_seconds)
# #     except KeyboardInterrupt:
# #         print("\nStopped by user. Streaming checkpoint saved.")
# #     finally:
# #         producer.close()

# #     print(f"New events sent in this run/session total: {emitted_events:,}")
# #     print(f"Next interval to resume: {next_interval}")
# #     return 0


# # if __name__ == "__main__":
# #     raise SystemExit(main())




# from __future__ import annotations

# import argparse
# import importlib.util
# import json
# import os
# import sys
# import time
# from datetime import datetime, timezone
# from pathlib import Path
# from typing import Any

# from azure.eventhub import EventData, EventHubProducerClient
# from azure.storage.blob import BlobServiceClient
# from dotenv import load_dotenv


# def load_generator(path: Path):
#     spec = importlib.util.spec_from_file_location("trackpulse_historical_generator", path)
#     if spec is None or spec.loader is None:
#         raise RuntimeError(f"Cannot load generator module: {path}")
#     module = importlib.util.module_from_spec(spec)
#     sys.modules[spec.name] = module
#     spec.loader.exec_module(module)
#     return module


# def read_json(path: Path) -> dict[str, Any]:
#     with path.open("r", encoding="utf-8") as f:
#         return json.load(f)


# def atomic_json(path: Path, obj: dict[str, Any]) -> None:
#     temp = path.with_suffix(path.suffix + ".tmp")
#     with temp.open("w", encoding="utf-8") as f:
#         json.dump(obj, f, ensure_ascii=False, indent=2)
#         f.flush()
#         os.fsync(f.fileno())
#     temp.replace(path)


# def save_cloud_checkpoint(blob_client, obj: dict[str, Any]) -> None:
#     """Atomically replace the logical checkpoint blob with the latest JSON state."""
#     payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
#     blob_client.upload_blob(payload, overwrite=True, content_type="application/json")


# def read_cloud_checkpoint(blob_client) -> dict[str, Any] | None:
#     if not blob_client.exists():
#         return None
#     return json.loads(blob_client.download_blob().readall())


# def main() -> int:
#     load_dotenv(override=False)
#     parser = argparse.ArgumentParser(
#         description="Continue TrackPulse telemetry after historical data and send new events to Azure Event Hubs."
#     )
#     parser.add_argument("--historical-output", type=Path, required=True)
#     parser.add_argument("--generator", type=Path, required=True,
#                         help="Path to generate_historical_data_trackpulse.py")
#     parser.add_argument("--sleep-seconds", type=float, default=1.0,
#                         help="Wall-clock pause after each fleet batch; 0 means send as fast as possible.")
#     parser.add_argument("--batch-size", type=int, default=5000,
#                         help="Events per send batch. Use 5000 to send one full fleet interval at a time.")
#     parser.add_argument("--max-events", type=int, default=0,
#                         help="Stop after this many new events; 0 means run until interrupted.")
#     parser.add_argument(
#         "--checkpoint", type=Path, default=None,
#         help="Optional local mirror/backup path; Azure Blob remains the source of truth."
#     )
#     parser.add_argument(
#         "--cloud-checkpoint-blob",
#         default="checkpoints/producer/stream_checkpoint.json",
#         help="Blob path inside the checkpoint container."
#     )
#     parser.add_argument(
#         "--checkpoint-container", default="trackpulse-data",
#         help="Azure Storage container used for the producer checkpoint."
#     )
#     args = parser.parse_args()

#     if args.sleep_seconds < 0 or args.batch_size < 1 or args.max_events < 0:
#         parser.error("sleep-seconds and max-events must be >= 0; batch-size must be >= 1")

#     storage_connection = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
#     if not storage_connection:
#         parser.error("Set AZURE_STORAGE_CONNECTION_STRING in .env.")

#     connection = os.getenv("EVENT_HUB_CONNECTION_STR")
#     hub_name = os.getenv("EVENT_HUB_NAME")
#     if not connection or not hub_name:
#         parser.error("Set EVENT_HUB_CONNECTION_STR and EVENT_HUB_NAME environment variables first.")

#     hist_dir = args.historical_output.resolve()
#     manifest_path = hist_dir / "manifest.json"
#     historical_checkpoint_path = hist_dir / "checkpoint.json"
#     if not manifest_path.exists() or not historical_checkpoint_path.exists():
#         parser.error("Historical manifest/checkpoint not found. Generate historical data first.")

#     manifest = read_json(manifest_path)
#     checkpoint = read_json(historical_checkpoint_path)
#     if manifest.get("status") != "completed":
#         parser.error(f"Historical generation is not completed (status={manifest.get('status')!r}).")
#     config = manifest.get("config", {})
#     vehicles = int(config.get("vehicles", 0))
#     target_events = int(config.get("target_events", 0))
#     interval_seconds = int(config.get("interval_seconds", 0))
#     if vehicles < 1 or interval_seconds < 1:
#         parser.error("Historical manifest has invalid vehicles/interval_seconds.")
#     if int(manifest.get("events", -1)) != target_events:
#         parser.error("Manifest event count does not equal configured target_events.")
#     next_interval = int(checkpoint.get("next_interval", -1))
#     if next_interval != target_events // vehicles:
#         parser.error(
#             f"Checkpoint is not at the end of history: next_interval={next_interval}, "
#             f"expected={target_events // vehicles}."
#         )
#     states = checkpoint.get("states")
#     if not isinstance(states, list) or len(states) != vehicles:
#         parser.error("Historical checkpoint does not contain all vehicle states.")

#     start_text = config.get("start_utc")
#     if not start_text:
#         parser.error("Historical manifest does not contain start_utc.")
#     start = datetime.fromisoformat(start_text.replace("Z", "+00:00")).astimezone(timezone.utc)
#     seed = int(config.get("seed", 42))
#     total_intervals_est = max(next_interval + 100000, 1)
#     generator = load_generator(args.generator.resolve())

#     # Azure Blob is the authoritative producer checkpoint. A local copy is optional.
#     checkpoint_path = (args.checkpoint or (hist_dir / "stream_checkpoint.json")).resolve()
#     blob_service = BlobServiceClient.from_connection_string(storage_connection)
#     checkpoint_blob = blob_service.get_blob_client(
#         container=args.checkpoint_container,
#         blob=args.cloud_checkpoint_blob,
#     )

#     stream_state = read_cloud_checkpoint(checkpoint_blob)
#     if stream_state is not None:
#         if stream_state.get("historical_output") != str(hist_dir):
#             parser.error("Cloud streaming checkpoint belongs to a different historical output.")
#         if stream_state.get("historical_events") != target_events:
#             parser.error("Cloud streaming checkpoint historical event count does not match.")
#         if not isinstance(stream_state.get("states"), list) or len(stream_state["states"]) != vehicles:
#             parser.error("Cloud streaming checkpoint has invalid vehicle states.")
#         states = stream_state["states"]
#         next_interval = int(stream_state["next_interval"])
#         emitted_events = int(stream_state.get("emitted_events", 0))
#         print("Loaded producer checkpoint from Azure Storage.")
#     else:
#         emitted_events = 0
#         stream_state = {
#             "version": 1,
#             "historical_output": str(hist_dir),
#             "historical_events": target_events,
#             "next_interval": next_interval,
#             "emitted_events": emitted_events,
#             "states": states,
#             "created_at_utc": datetime.now(timezone.utc).isoformat(),
#         }
#         save_cloud_checkpoint(checkpoint_blob, stream_state)
#         print("Initialized producer checkpoint in Azure Storage from historical checkpoint.")

#     # Keep a local mirror for convenience, but never use it as the authoritative state.
#     atomic_json(checkpoint_path, stream_state)

#     producer = EventHubProducerClient.from_connection_string(
#         conn_str=connection,
#         eventhub_name=hub_name,
#     )
#     print(f"Historical events: {target_events:,}")
#     print(f"Continuing from interval {next_interval}; first new event index is {next_interval}.")
#     print(f"Cloud checkpoint: {args.checkpoint_container}/{args.cloud_checkpoint_blob}")
#     print(f"Local checkpoint mirror: {checkpoint_path}")
#     print("Press Ctrl+C to stop. Restarting resumes from the Azure checkpoint.")

#     try:
#         with producer:
#             while True:
#                 if args.max_events and emitted_events >= args.max_events:
#                     break

#                 # Snapshot the interval's starting state. The durable checkpoint advances
#                 # only after every vehicle event for this interval has been acknowledged.
#                 interval_state_before = json.loads(json.dumps(states))
#                 interval_events_sent = 0
#                 batch = producer.create_batch()

#                 for p in states:
#                     if args.max_events and emitted_events + interval_events_sent >= args.max_events:
#                         break
#                     event, _labels = generator.make_event(
#                         p, next_interval, start, interval_seconds, seed, total_intervals_est
#                     )
#                     payload = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
#                     try:
#                         batch.add(EventData(payload))
#                     except ValueError:
#                         if len(batch) == 0:
#                             raise RuntimeError(
#                                 "A single telemetry event is larger than the Event Hubs batch limit."
#                             )
#                         producer.send_batch(batch)
#                         interval_events_sent += len(batch)
#                         batch = producer.create_batch()
#                         batch.add(EventData(payload))

#                 if len(batch):
#                     producer.send_batch(batch)
#                     interval_events_sent += len(batch)

#                 emitted_events += interval_events_sent
#                 if interval_events_sent == vehicles:
#                     next_interval += 1
#                     # Save updated state only after all events for the interval were sent.
#                     stream_state = {
#                         "version": 1, "historical_output": str(hist_dir),
#                         "historical_events": target_events, "next_interval": next_interval,
#                         "emitted_events": emitted_events, "states": states,
#                         "updated_at_utc": datetime.now(timezone.utc).isoformat(),
#                     }
#                     save_cloud_checkpoint(checkpoint_blob, stream_state)
#                     atomic_json(checkpoint_path, stream_state)
#                     print(
#                         f"Checkpoint saved to Azure: next_interval={next_interval}, "
#                         f"emitted_events={emitted_events:,}"
#                     )
#                 else:
#                     # A max-events cutoff in the middle of a fleet interval is intentionally
#                     # not checkpointed as complete; restore state to the interval boundary.
#                     states = interval_state_before
#                     stream_state = {
#                         "version": 1, "historical_output": str(hist_dir),
#                         "historical_events": target_events, "next_interval": next_interval,
#                         "emitted_events": emitted_events, "states": states,
#                         "updated_at_utc": datetime.now(timezone.utc).isoformat(),
#                         "warning": "Partial fleet interval may be resent on restart; deduplicate by event_id.",
#                     }
#                     save_cloud_checkpoint(checkpoint_blob, stream_state)
#                     atomic_json(checkpoint_path, stream_state)
#                     print(
#                         "Checkpoint saved with partial-interval warning; "
#                         "deduplicate by event_id."
#                     )
#                 if args.max_events and emitted_events >= args.max_events:
#                     break
#                 if args.sleep_seconds:
#                     time.sleep(args.sleep_seconds)
#     except KeyboardInterrupt:
#         print("\nStopped by user. Streaming checkpoint saved.")
#     finally:
#         producer.close()
#         blob_service.close()

#     print(f"New events sent in this run/session total: {emitted_events:,}")
#     print(f"Next interval to resume: {next_interval}")
#     return 0


# if __name__ == "__main__":
#     raise SystemExit(main())




from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from azure.core.exceptions import ResourceExistsError
from azure.eventhub import EventData, EventHubProducerClient
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv


LOGGER = logging.getLogger("trackpulse.producer")
CHECKPOINT_VERSION = 2
DEFAULT_CHECKPOINT_CONTAINER = "trackpulse-data"
DEFAULT_CLOUD_CHECKPOINT_BLOB = "checkpoints/producer/stream_checkpoint.json"
DEFAULT_CLOUD_LOCK_BLOB = "checkpoints/producer/stream_checkpoint.lock"
DEFAULT_SIMULATION_HORIZON_EXTRA_INTERVALS = 100_000
LEASE_DURATION_SECONDS = 60
LEASE_RENEW_INTERVAL_SECONDS = 20


class CheckpointError(RuntimeError):
    """Raised when a producer checkpoint is missing, invalid, or inconsistent."""


class CheckpointLease:
    """A renewable Azure Blob lease used as a single-producer lock."""

    def __init__(self, lock_blob_client: Any) -> None:
        self._blob = lock_blob_client
        self._lease: Any | None = None
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread: threading.Thread | None = None
        self._renew_error: Exception | None = None

    def acquire(self) -> None:
        # The lock blob is intentionally retained after release; it is a zero-byte lock file.
        if not self._blob.exists():
            try:
                self._blob.upload_blob(b"", overwrite=False)
            except ResourceExistsError:
                # Another process may have created the blob between exists() and upload_blob().
                pass

        self._lease = self._blob.acquire_lease(lease_duration=LEASE_DURATION_SECONDS)
        self._thread = threading.Thread(
            target=self._renew_loop,
            name="trackpulse-checkpoint-lease-renewal",
            daemon=True,
        )
        self._thread.start()
        LOGGER.info("Acquired producer lock (Blob lease).")

    def _renew_loop(self) -> None:
        while not self._stop.wait(LEASE_RENEW_INTERVAL_SECONDS):
            try:
                if self._lease is None:
                    raise RuntimeError("Blob lease was not initialized.")
                self._lease.renew()
            except Exception as exc:  # pragma: no cover - depends on remote Azure failures
                self._renew_error = exc
                self._lost.set()
                LOGGER.error(
                    "Could not renew the producer lock. Processing will stop before the next "
                    "safe checkpoint operation. Details: %s",
                    exc,
                )
                return

    def assert_held(self) -> None:
        if self._lost.is_set():
            raise RuntimeError(
                "The Azure Blob producer lock was lost; refusing further sends/checkpoint writes."
            ) from self._renew_error

    def release(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._lease is not None:
            try:
                self._lease.release()
                LOGGER.info("Released producer lock.")
            except Exception as exc:  # best-effort cleanup; lease will expire if process is gone
                LOGGER.warning("Could not explicitly release the producer lock: %s", exc)
            finally:
                self._lease = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_generator(path: Path) -> Any:
    """Load the historical generator module without changing Python import paths."""
    if not path.is_file():
        raise FileNotFoundError(f"Historical generator file was not found: {path}")
    spec = importlib.util.spec_from_file_location("trackpulse_historical_generator", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load historical generator module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    if not callable(getattr(module, "make_event", None)):
        raise RuntimeError(f"The generator module does not expose make_event(): {path}")
    return module


def read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object in {path}.")
    return data


def atomic_json(path: Path, obj: dict[str, Any]) -> None:
    """Atomically replace a local JSON file without risking a half-written mirror."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(obj, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        # If writing failed, do not leave a confusing temporary checkpoint behind.
        try:
            if temporary_path.exists():
                temporary_path.unlink()
        except OSError:
            pass


def save_cloud_checkpoint(blob_client: Any, obj: dict[str, Any]) -> None:
    """Replace the checkpoint JSON blob with the latest state."""
    payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    blob_client.upload_blob(
        payload,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json; charset=utf-8"),
    )


def read_cloud_checkpoint(blob_client: Any) -> dict[str, Any] | None:
    if not blob_client.exists():
        return None
    try:
        checkpoint = json.loads(blob_client.download_blob().readall())
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CheckpointError(f"Cloud checkpoint is not valid JSON: {exc}") from exc
    if not isinstance(checkpoint, dict):
        raise CheckpointError("Cloud checkpoint must contain a JSON object.")
    return checkpoint


def first_non_empty_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()
    return None


def load_environment() -> None:
    # Keep environment variables injected by the shell/IDE authoritative.
    # load_dotenv() defaults to override=False.
    load_dotenv(override=False)


def parse_utc_datetime(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 datetime: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone, such as Z or +00:00.")
    return parsed.astimezone(timezone.utc)


def validate_historical_inputs(
    historical_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any], int, int, int, datetime, int, int]:
    """Read and validate historical artifacts. Never modifies them."""
    manifest_path = historical_dir / "manifest.json"
    historical_checkpoint_path = historical_dir / "checkpoint.json"
    if not manifest_path.is_file() or not historical_checkpoint_path.is_file():
        raise FileNotFoundError(
            "Historical manifest.json/checkpoint.json were not found under "
            f"{historical_dir}. Generate/verify historical data first."
        )

    manifest = read_json(manifest_path)
    historical_checkpoint = read_json(historical_checkpoint_path)
    if manifest.get("status") != "completed":
        raise ValueError(
            f"Historical generation is not complete (manifest status={manifest.get('status')!r})."
        )

    config = manifest.get("config")
    if not isinstance(config, dict):
        raise ValueError("Historical manifest is missing its config object.")

    try:
        vehicles = int(config.get("vehicles", 0))
        target_events = int(config.get("target_events", 0))
        interval_seconds = int(config.get("interval_seconds", 0))
        seed = int(config.get("seed", 42))
        manifest_events = int(manifest.get("events", -1))
        historical_next_interval = int(historical_checkpoint.get("next_interval", -1))
    except (TypeError, ValueError) as exc:
        raise ValueError("Historical manifest/checkpoint contains invalid numeric fields.") from exc

    if vehicles < 1 or target_events < 1 or interval_seconds < 1:
        raise ValueError("Historical vehicles, target_events, and interval_seconds must be positive.")
    if target_events % vehicles != 0:
        raise ValueError(
            "Historical target_events must contain complete fleet intervals; expected an exact "
            "multiple of vehicles."
        )
    if manifest_events != target_events:
        raise ValueError(
            f"Manifest events ({manifest_events:,}) do not match target_events ({target_events:,})."
        )

    expected_next_interval = target_events // vehicles
    if historical_next_interval != expected_next_interval:
        raise ValueError(
            "Historical checkpoint is not at the end of the historical dataset: "
            f"next_interval={historical_next_interval}, expected={expected_next_interval}."
        )

    states = historical_checkpoint.get("states")
    if not isinstance(states, list) or len(states) != vehicles:
        raise ValueError(
            f"Historical checkpoint must contain exactly {vehicles:,} vehicle states."
        )
    validate_vehicle_states(states, vehicles, "historical checkpoint")

    start_text = config.get("start_utc")
    if not isinstance(start_text, str) or not start_text.strip():
        raise ValueError("Historical manifest config.start_utc is missing.")
    start = parse_utc_datetime(start_text, "config.start_utc")

    return (
        manifest,
        historical_checkpoint,
        vehicles,
        target_events,
        interval_seconds,
        start,
        seed,
        expected_next_interval,
    )


def validate_vehicle_states(states: list[Any], expected_count: int, source_name: str) -> None:
    if len(states) != expected_count:
        raise CheckpointError(
            f"{source_name} contains {len(states)} states; expected {expected_count}."
        )
    seen_ids: set[str] = set()
    for index, state in enumerate(states):
        if not isinstance(state, dict):
            raise CheckpointError(f"{source_name} state #{index} is not a JSON object.")
        vehicle_id = state.get("vehicle_id")
        if not isinstance(vehicle_id, str) or not vehicle_id.strip():
            raise CheckpointError(f"{source_name} state #{index} has no valid vehicle_id.")
        if vehicle_id in seen_ids:
            raise CheckpointError(f"{source_name} contains duplicate vehicle_id {vehicle_id!r}.")
        seen_ids.add(vehicle_id)
        # These state fields are used/updated by make_event(). Fail early rather than mid-stream.
        required_generator_fields = (
            "rng_seed", "speed", "rpm", "engine_temp", "cargo_temp", "odometer",
            "route_progress_km", "route_idx", "kind", "phase", "cruise", "fuel_factor",
            "engine_base", "cargo_target", "driver_id", "route_id",
        )
        missing = [field for field in required_generator_fields if field not in state]
        if missing:
            raise CheckpointError(
                f"{source_name} state for {vehicle_id} is missing generator fields: {', '.join(missing)}."
            )


def normalize_stream_checkpoint(
    raw_checkpoint: dict[str, Any],
    *,
    historical_dir: Path,
    target_events: int,
    vehicles: int,
    historical_next_interval: int,
) -> tuple[dict[str, Any], list[str]]:
    """Validate/migrate legacy v1 checkpoints into the current v2 format in memory."""
    warnings: list[str] = []
    raw_version = raw_checkpoint.get("version", 1)
    try:
        version = int(raw_version)
        next_interval = int(raw_checkpoint.get("next_interval", -1))
        legacy_emitted_events = int(raw_checkpoint.get("emitted_events", 0))
    except (TypeError, ValueError) as exc:
        raise CheckpointError("Cloud checkpoint has invalid version/next_interval/emitted_events values.") from exc

    if version not in (1, CHECKPOINT_VERSION):
        raise CheckpointError(
            f"Unsupported producer checkpoint version {version}; supported versions are 1 and {CHECKPOINT_VERSION}."
        )
    if raw_checkpoint.get("historical_output") != str(historical_dir):
        raise CheckpointError(
            "Cloud producer checkpoint belongs to a different historical_output path. "
            "Do not delete/replace it until you verify which run it belongs to."
        )
    if raw_checkpoint.get("historical_events") != target_events:
        raise CheckpointError("Cloud producer checkpoint historical_events does not match the manifest.")
    if next_interval < historical_next_interval:
        raise CheckpointError(
            f"Cloud checkpoint next_interval={next_interval} is earlier than historical boundary "
            f"{historical_next_interval}. Refusing to rewind automatically."
        )

    states = raw_checkpoint.get("states")
    if not isinstance(states, list):
        raise CheckpointError("Cloud producer checkpoint has no valid states array.")
    validate_vehicle_states(states, vehicles, "cloud producer checkpoint")

    committed_events = (next_interval - historical_next_interval) * vehicles
    if committed_events < 0:
        raise CheckpointError("Cloud producer checkpoint implies a negative committed event count.")

    # v1 used emitted_events for both completed and partial intervals. The number of fully
    # checkpointed events is unambiguous from next_interval; preserve the old value as an audit
    # field when it differs, and warn because partial sends may be replayed on restart.
    if version == 1 and legacy_emitted_events != committed_events:
        warnings.append(
            "Legacy checkpoint's emitted_events did not match its committed interval boundary. "
            "Normalized the committed count from next_interval; previously accepted events from a "
            "partial interval may be replayed and must be deduplicated by event_id."
        )

    if version == CHECKPOINT_VERSION and legacy_emitted_events != committed_events:
        raise CheckpointError(
            "Version 2 checkpoint is inconsistent: emitted_events must equal "
            "(next_interval - historical_next_interval) * vehicles. "
            "Refusing to silently repair a current-format checkpoint."
        )

    created_at = raw_checkpoint.get("created_at_utc") or utc_now_iso()
    try:
        total_intervals_est = int(raw_checkpoint.get("total_intervals_est", 0))
    except (TypeError, ValueError):
        total_intervals_est = 0
    if total_intervals_est < next_interval:
        # Persist once so the generator's scheduled synthetic incidents don't shift on restarts.
        total_intervals_est = max(next_interval + DEFAULT_SIMULATION_HORIZON_EXTRA_INTERVALS, 1)

    normalized: dict[str, Any] = {
        "version": CHECKPOINT_VERSION,
        "checkpoint_type": "trackpulse_eventhub_producer",
        "historical_output": str(historical_dir),
        "historical_events": target_events,
        "historical_next_interval": historical_next_interval,
        "next_interval": next_interval,
        "emitted_events": committed_events,
        "total_intervals_est": total_intervals_est,
        "states": states,
        "created_at_utc": created_at,
        "updated_at_utc": raw_checkpoint.get("updated_at_utc") or utc_now_iso(),
    }
    if version == 1 and legacy_emitted_events != committed_events:
        normalized["legacy_emitted_events_at_migration"] = legacy_emitted_events
        normalized["recovery_note"] = warnings[-1]
    elif raw_checkpoint.get("legacy_emitted_events_at_migration") is not None:
        normalized["legacy_emitted_events_at_migration"] = raw_checkpoint["legacy_emitted_events_at_migration"]
        if raw_checkpoint.get("recovery_note"):
            normalized["recovery_note"] = raw_checkpoint["recovery_note"]

    return normalized, warnings


def create_initial_checkpoint(
    *,
    historical_dir: Path,
    target_events: int,
    historical_next_interval: int,
    vehicles: int,
    states: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "version": CHECKPOINT_VERSION,
        "checkpoint_type": "trackpulse_eventhub_producer",
        "historical_output": str(historical_dir),
        "historical_events": target_events,
        "historical_next_interval": historical_next_interval,
        "next_interval": historical_next_interval,
        "emitted_events": 0,
        "total_intervals_est": max(
            historical_next_interval + DEFAULT_SIMULATION_HORIZON_EXTRA_INTERVALS, 1
        ),
        "states": copy.deepcopy(states),
        "created_at_utc": utc_now_iso(),
        "updated_at_utc": utc_now_iso(),
    }


def persist_checkpoint(
    *,
    checkpoint_blob: Any,
    checkpoint_path: Path,
    checkpoint: dict[str, Any],
    lease_guard: CheckpointLease,
) -> None:
    """Write cloud first (source of truth), then update the local convenience mirror."""
    lease_guard.assert_held()
    save_cloud_checkpoint(checkpoint_blob, checkpoint)
    atomic_json(checkpoint_path, checkpoint)


def send_fleet_interval(
    *,
    producer: Any,
    generator: Any,
    states: list[dict[str, Any]],
    interval_index: int,
    start: datetime,
    interval_seconds: int,
    seed: int,
    total_intervals_est: int,
    batch_size: int,
    lease_guard: CheckpointLease,
) -> int:
    """Generate and publish one complete fleet interval; returns acknowledged event count.

    If any send fails, this function raises and the caller must not advance the checkpoint.
    Some earlier batches may already have reached Event Hubs; stable event_id supports downstream
    deduplication after restart.
    """
    batch = producer.create_batch()
    events_in_batch = 0
    acknowledged = 0

    def flush_current_batch() -> None:
        nonlocal batch, events_in_batch, acknowledged
        if events_in_batch == 0:
            return
        lease_guard.assert_held()
        count_to_commit = events_in_batch
        producer.send_batch(batch)
        acknowledged += count_to_commit
        batch = producer.create_batch()
        events_in_batch = 0

    for state in states:
        event, _labels = generator.make_event(
            state,
            interval_index,
            start,
            interval_seconds,
            seed,
            total_intervals_est,
        )
        if not isinstance(event, dict):
            raise ValueError(
                f"Generator returned a non-object event for vehicle {state.get('vehicle_id')} "
                f"at interval {interval_index}."
            )
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError(
                f"Generator returned an event without a valid event_id for vehicle "
                f"{state.get('vehicle_id')} at interval {interval_index}."
            )
        payload = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
        event_data = EventData(payload)

        try:
            batch.add(event_data)
        except ValueError as exc:
            # EventDataBatch raises ValueError when adding would exceed the service/batch limit.
            if events_in_batch == 0:
                raise ValueError(
                    f"A single telemetry event ({event_id}) exceeds the Event Hubs batch size limit."
                ) from exc
            flush_current_batch()
            try:
                batch.add(event_data)
            except ValueError as second_exc:
                raise ValueError(
                    f"A single telemetry event ({event_id}) exceeds the Event Hubs batch size limit."
                ) from second_exc
            events_in_batch += 1
        else:
            events_in_batch += 1

        if events_in_batch >= batch_size:
            flush_current_batch()

    flush_current_batch()
    if acknowledged != len(states):
        raise RuntimeError(
            f"Only {acknowledged} of {len(states)} fleet events were acknowledged for interval "
            f"{interval_index}; checkpoint will not advance."
        )
    return acknowledged


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Continue TrackPulse telemetry after historical data and publish it to Azure Event Hubs. "
            "Azure Blob Storage is the authoritative producer checkpoint."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--historical-output",
        type=Path,
        required=True,
        help="Directory containing the completed historical manifest.json and checkpoint.json.",
    )
    parser.add_argument(
        "--generator",
        type=Path,
        required=True,
        help="Path to generate_historical_data_trackpulse.py (must expose make_event()).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Pause after each fully committed fleet interval; 0 sends continuously without a pause.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Maximum number of events per Event Hubs send batch; service byte limits may flush earlier.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help=(
            "Maximum NEW events to publish in this run; 0 runs until interrupted. Must be 0 or a "
            "multiple of the vehicle count so checkpoints only commit complete fleet intervals."
        ),
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=None,
        help="Optional local checkpoint mirror path; never used as the source of truth.",
    )
    parser.add_argument(
        "--cloud-checkpoint-blob",
        default=DEFAULT_CLOUD_CHECKPOINT_BLOB,
        help="Blob path inside the checkpoint container for the authoritative producer checkpoint.",
    )
    parser.add_argument(
        "--checkpoint-container",
        default=DEFAULT_CHECKPOINT_CONTAINER,
        help="Existing Azure Storage container used for the producer checkpoint and lock.",
    )
    parser.add_argument(
        "--cloud-lock-blob",
        default=DEFAULT_CLOUD_LOCK_BLOB,
        help="Blob path used as the renewable single-producer lock.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate local historical files and generator without connecting or sending to Azure.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    load_environment()
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.sleep_seconds < 0:
        parser.error("--sleep-seconds must be >= 0.")
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1.")
    if args.max_events < 0:
        parser.error("--max-events must be >= 0.")
    if not args.checkpoint_container.strip():
        parser.error("--checkpoint-container must not be empty.")
    if not args.cloud_checkpoint_blob.strip() or not args.cloud_lock_blob.strip():
        parser.error("Cloud checkpoint/lock blob paths must not be empty.")
    if args.cloud_checkpoint_blob.strip("/") == args.cloud_lock_blob.strip("/"):
        parser.error("--cloud-checkpoint-blob and --cloud-lock-blob must be different blob paths.")

    historical_dir = args.historical_output.expanduser().resolve()
    manifest_path = historical_dir / "manifest.json"
    historical_checkpoint_path = historical_dir / "checkpoint.json"
    try:
        (
            _manifest,
            historical_checkpoint,
            vehicles,
            target_events,
            interval_seconds,
            start,
            seed,
            historical_next_interval,
        ) = validate_historical_inputs(historical_dir)
        generator_module = load_generator(args.generator.expanduser().resolve())
    except Exception as exc:
        parser.error(str(exc))

    checkpoint_path = (args.checkpoint or (historical_dir / "stream_checkpoint.json")).expanduser().resolve()
    if checkpoint_path in (manifest_path.resolve(), historical_checkpoint_path.resolve()):
        parser.error(
            "The local checkpoint mirror cannot overwrite manifest.json or the historical checkpoint.json."
        )
    if args.max_events and args.max_events % vehicles != 0:
        parser.error(
            f"--max-events must be 0 or a multiple of the vehicle count ({vehicles:,}); "
            "this prevents stopping inside a fleet interval."
        )

    LOGGER.info("Validated historical data: %s events, %s vehicles, %s-second intervals.",
                f"{target_events:,}", f"{vehicles:,}", interval_seconds)
    LOGGER.info("Historical end boundary: next_interval=%s.", historical_next_interval)
    LOGGER.info("Historical checkpoint is read-only: %s", historical_checkpoint_path)
    LOGGER.info("Local checkpoint mirror: %s", checkpoint_path)

    if args.validate_only:
        LOGGER.info("VALIDATION ONLY: local historical artifacts and generator are valid; no Azure calls or events were sent.")
        return 0

    storage_connection = first_non_empty_env("AZURE_STORAGE_CONNECTION_STRING")
    eventhub_connection = first_non_empty_env("EVENT_HUB_CONNECTION_STR", "EVENT_HUB_CONNECTION_STRING")
    eventhub_name = first_non_empty_env("EVENT_HUB_NAME")
    missing = []
    if not storage_connection:
        missing.append("AZURE_STORAGE_CONNECTION_STRING")
    if not eventhub_connection:
        missing.append("EVENT_HUB_CONNECTION_STR (or EVENT_HUB_CONNECTION_STRING)")
    if not eventhub_name:
        missing.append("EVENT_HUB_NAME")
    if missing:
        parser.error("Missing required environment setting(s): " + ", ".join(missing))

    blob_service: Any | None = None
    producer: Any | None = None
    lease_guard: CheckpointLease | None = None
    run_events_acknowledged = 0
    run_id = f"producer-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-pid{os.getpid()}"
    return_code = 0

    try:
        blob_service = BlobServiceClient.from_connection_string(storage_connection)
        container_client = blob_service.get_container_client(args.checkpoint_container)
        if not container_client.exists():
            raise RuntimeError(
                f"Checkpoint container {args.checkpoint_container!r} does not exist. "
                "Create/verify it in Azure Storage; this script intentionally does not create containers automatically."
            )

        checkpoint_blob = blob_service.get_blob_client(
            container=args.checkpoint_container,
            blob=args.cloud_checkpoint_blob,
        )
        lock_blob = blob_service.get_blob_client(
            container=args.checkpoint_container,
            blob=args.cloud_lock_blob,
        )

        lease_guard = CheckpointLease(lock_blob)
        try:
            lease_guard.acquire()
        except Exception as exc:
            raise RuntimeError(
                "Could not acquire the producer lock. Another producer may already be running, "
                "or the Storage connection lacks blob lease permissions. Do not start a second producer."
            ) from exc

        cloud_checkpoint = read_cloud_checkpoint(checkpoint_blob)
        if cloud_checkpoint is None:
            cloud_checkpoint = create_initial_checkpoint(
                historical_dir=historical_dir,
                target_events=target_events,
                historical_next_interval=historical_next_interval,
                vehicles=vehicles,
                states=historical_checkpoint["states"],
            )
            persist_checkpoint(
                checkpoint_blob=checkpoint_blob,
                checkpoint_path=checkpoint_path,
                checkpoint=cloud_checkpoint,
                lease_guard=lease_guard,
            )
            LOGGER.info("Initialized producer checkpoint in Azure Storage from the historical checkpoint.")
        else:
            cloud_checkpoint, migration_warnings = normalize_stream_checkpoint(
                cloud_checkpoint,
                historical_dir=historical_dir,
                target_events=target_events,
                vehicles=vehicles,
                historical_next_interval=historical_next_interval,
            )
            # Migrate v1 checkpoints or normalize the local mirror before publishing anything.
            persist_checkpoint(
                checkpoint_blob=checkpoint_blob,
                checkpoint_path=checkpoint_path,
                checkpoint=cloud_checkpoint,
                lease_guard=lease_guard,
            )
            for warning in migration_warnings:
                LOGGER.warning("Checkpoint compatibility/recovery warning: %s", warning)
            if cloud_checkpoint.get("recovery_note") and not migration_warnings:
                LOGGER.warning("Checkpoint recovery note: %s", cloud_checkpoint["recovery_note"])
            LOGGER.info("Loaded authoritative producer checkpoint from Azure Storage.")

        next_interval = int(cloud_checkpoint["next_interval"])
        emitted_events_total = int(cloud_checkpoint["emitted_events"])
        states = cloud_checkpoint["states"]
        total_intervals_est = int(cloud_checkpoint["total_intervals_est"])
        # Keep migration/recovery audit metadata across subsequent interval commits.
        checkpoint_audit_fields = {
            key: cloud_checkpoint[key]
            for key in ("legacy_emitted_events_at_migration", "recovery_note")
            if key in cloud_checkpoint
        }
        LOGGER.info("Run ID: %s", run_id)
        LOGGER.info("Event Hub: %s", eventhub_name)
        LOGGER.info("Cloud checkpoint: %s/%s", args.checkpoint_container, args.cloud_checkpoint_blob)
        LOGGER.info("Next interval: %s; next event IDs end with interval index %07d.",
                    next_interval, next_interval)
        LOGGER.info("Previously committed streaming events: %s", f"{emitted_events_total:,}")
        LOGGER.info("Configured max NEW events this run: %s",
                    f"{args.max_events:,}" if args.max_events else "unlimited (Ctrl+C to stop)")
        LOGGER.info(
            "Delivery is at-least-once across crashes; deduplicate downstream by event_id."
        )

        producer = EventHubProducerClient.from_connection_string(
            conn_str=eventhub_connection,
            eventhub_name=eventhub_name,
        )

        while True:
            lease_guard.assert_held()
            if args.max_events and run_events_acknowledged >= args.max_events:
                break

            interval_index = next_interval
            acknowledged = send_fleet_interval(
                producer=producer,
                generator=generator_module,
                states=states,
                interval_index=interval_index,
                start=start,
                interval_seconds=interval_seconds,
                seed=seed,
                total_intervals_est=total_intervals_est,
                batch_size=args.batch_size,
                lease_guard=lease_guard,
            )
            if acknowledged != vehicles:
                # Extra guard: never commit a partial fleet interval.
                raise RuntimeError(
                    f"Interval {interval_index} acknowledged {acknowledged} events; expected {vehicles}."
                )

            # The checkpoint advances only after all fleet events were acknowledged by Event Hubs.
            next_interval += 1
            run_events_acknowledged += acknowledged
            emitted_events_total = (next_interval - historical_next_interval) * vehicles
            cloud_checkpoint = {
                "version": CHECKPOINT_VERSION,
                "checkpoint_type": "trackpulse_eventhub_producer",
                "historical_output": str(historical_dir),
                "historical_events": target_events,
                "historical_next_interval": historical_next_interval,
                "next_interval": next_interval,
                "emitted_events": emitted_events_total,
                "total_intervals_est": total_intervals_est,
                "states": states,
                "created_at_utc": cloud_checkpoint.get("created_at_utc", utc_now_iso()),
                "updated_at_utc": utc_now_iso(),
                "last_completed_interval": interval_index,
                "last_completed_interval_event_count": vehicles,
                "last_completed_run_id": run_id,
            }
            # Preserve migration audit fields until an operator has reviewed the checkpoint.
            cloud_checkpoint.update(checkpoint_audit_fields)

            persist_checkpoint(
                checkpoint_blob=checkpoint_blob,
                checkpoint_path=checkpoint_path,
                checkpoint=cloud_checkpoint,
                lease_guard=lease_guard,
            )
            LOGGER.info(
                "Committed interval %s | events in this run: %s | total committed events: %s | next interval: %s",
                interval_index,
                f"{run_events_acknowledged:,}",
                f"{emitted_events_total:,}",
                next_interval,
            )

            if args.max_events and run_events_acknowledged >= args.max_events:
                break
            if args.sleep_seconds:
                time.sleep(args.sleep_seconds)

        LOGGER.info("Producer run completed normally.")

    except KeyboardInterrupt:
        return_code = 130
        LOGGER.warning(
            "Interrupted by user. Last fully committed checkpoint is authoritative. If interruption "
            "occurred mid-interval, some events from that interval may be resent after restart; deduplicate by event_id."
        )
    except Exception as exc:
        return_code = 1
        LOGGER.exception(
            "Producer stopped with an error. No checkpoint was advanced for an incomplete interval. "
            "Events already accepted by Event Hubs may be replayed after restart; deduplicate by event_id. "
            "Cause: %s",
            exc,
        )
    finally:
        if producer is not None:
            try:
                producer.close()
            except Exception as exc:  # pragma: no cover - best-effort cleanup
                LOGGER.warning("Error while closing Event Hubs producer: %s", exc)
        if lease_guard is not None:
            lease_guard.release()
        if blob_service is not None:
            try:
                blob_service.close()
            except Exception as exc:  # pragma: no cover - best-effort cleanup
                LOGGER.warning("Error while closing Azure Blob service client: %s", exc)

    LOGGER.info("Events in fully committed intervals this run: %s", f"{run_events_acknowledged:,}")
    if return_code == 0:
        LOGGER.info("Next interval to resume: %s", locals().get("next_interval", "unchanged"))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())

