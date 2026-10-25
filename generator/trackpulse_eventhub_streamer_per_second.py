from __future__ import annotations

import argparse
import copy
from concurrent.futures import Future, ThreadPoolExecutor
import importlib.util
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from azure.core.exceptions import ResourceExistsError
from azure.eventhub import EventData, EventHubProducerClient, TransportType
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv


LOGGER = logging.getLogger("trackpulse.producer")
CHECKPOINT_VERSION = 3
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
    stream_interval_seconds: int,
) -> tuple[dict[str, Any], list[str]]:
    """Validate/migrate older checkpoints into the per-second-capable v3 format in memory."""
    warnings: list[str] = []
    raw_version = raw_checkpoint.get("version", 1)
    try:
        version = int(raw_version)
        next_interval = int(raw_checkpoint.get("next_interval", -1))
        legacy_emitted_events = int(raw_checkpoint.get("emitted_events", 0))
    except (TypeError, ValueError) as exc:
        raise CheckpointError("Cloud checkpoint has invalid version/next_interval/emitted_events values.") from exc

    if version not in (1, 2, CHECKPOINT_VERSION):
        raise CheckpointError(
            f"Unsupported producer checkpoint version {version}; supported versions are 1, 2, and {CHECKPOINT_VERSION}."
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

    if version in (2, CHECKPOINT_VERSION) and legacy_emitted_events != committed_events:
        raise CheckpointError(
            f"Version {version} checkpoint is inconsistent: emitted_events must equal "
            "(next_interval - historical_next_interval) * vehicles. "
            "Refusing to silently repair a current-format checkpoint."
        )

    raw_interval = raw_checkpoint.get("stream_interval_seconds")
    if raw_interval is None:
        if committed_events > 0:
            raise CheckpointError(
                "The existing production checkpoint has committed events but does not record the live "
                "stream interval. To avoid changing event timestamps or duplicating an unknown timeline, "
                "this per-second producer refuses to migrate it automatically. Inspect the checkpoint first."
            )
        stored_interval = stream_interval_seconds
        warnings.append(
            f"Initialized live stream cadence to {stream_interval_seconds}s because no production events "
            "had been committed in the legacy checkpoint."
        )
    else:
        try:
            stored_interval = int(raw_interval)
        except (TypeError, ValueError) as exc:
            raise CheckpointError("Cloud checkpoint stream_interval_seconds is invalid.") from exc
        if stored_interval != stream_interval_seconds:
            raise CheckpointError(
                f"Cloud checkpoint stream_interval_seconds={stored_interval} does not match requested "
                f"{stream_interval_seconds}. Do not change cadence in-place without reviewing the existing checkpoint."
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
        "stream_interval_seconds": stored_interval,
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
    stream_interval_seconds: int,
) -> dict[str, Any]:
    return {
        "version": CHECKPOINT_VERSION,
        "checkpoint_type": "trackpulse_eventhub_producer",
        "historical_output": str(historical_dir),
        "historical_events": target_events,
        "historical_next_interval": historical_next_interval,
        "stream_interval_seconds": stream_interval_seconds,
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



def validate_test_checkpoint(
    checkpoint: dict[str, Any],
    *,
    test_id: str,
    historical_dir: Path,
    target_events: int,
    vehicles: int,
    historical_next_interval: int,
) -> None:
    """Validate an isolated test checkpoint without modifying it."""
    if checkpoint.get("checkpoint_type") != "trackpulse_eventhub_test_producer":
        raise CheckpointError("The isolated test checkpoint has an unexpected checkpoint_type.")
    if checkpoint.get("test_id") != test_id:
        raise CheckpointError("The isolated test checkpoint belongs to a different --test-id.")
    if checkpoint.get("historical_output") != str(historical_dir):
        raise CheckpointError("The isolated test checkpoint belongs to a different historical dataset path.")
    if checkpoint.get("historical_events") != target_events:
        raise CheckpointError("The isolated test checkpoint historical event count does not match the manifest.")

    try:
        next_interval = int(checkpoint["next_interval"])
        next_vehicle_index = int(checkpoint["next_vehicle_index"])
        emitted_events = int(checkpoint["emitted_events"])
        total_intervals_est = int(checkpoint["total_intervals_est"])
    except (KeyError, TypeError, ValueError) as exc:
        raise CheckpointError("The isolated test checkpoint has invalid progress fields.") from exc

    if next_interval < historical_next_interval:
        raise CheckpointError("The test checkpoint is earlier than the historical boundary; refusing to rewind.")
    if not 0 <= next_vehicle_index < vehicles:
        raise CheckpointError(
            f"next_vehicle_index must be in [0, {vehicles - 1}], got {next_vehicle_index}."
        )
    if emitted_events != (next_interval - historical_next_interval) * vehicles + next_vehicle_index:
        raise CheckpointError("The isolated test checkpoint progress fields are inconsistent.")
    if total_intervals_est < next_interval:
        raise CheckpointError("The test checkpoint simulation horizon is earlier than next_interval.")

    states = checkpoint.get("states")
    if not isinstance(states, list):
        raise CheckpointError("The isolated test checkpoint has no states array.")
    validate_vehicle_states(states, vehicles, "isolated test checkpoint")


def run_isolated_test_mode(
    *,
    checkpoint_blob: Any,
    checkpoint_path: Path,
    producer: Any,
    generator: Any,
    historical_dir: Path,
    historical_checkpoint: dict[str, Any],
    target_events: int,
    vehicles: int,
    interval_seconds: int,
    start: datetime,
    seed: int,
    historical_next_interval: int,
    total_intervals_est_default: int,
    test_id: str,
    max_events: int,
    batch_size: int,
    batch_pause_seconds: float,
    lease_guard: CheckpointLease,
) -> tuple[int, int]:
    """Publish bounded test events using a dedicated resumable checkpoint.

    The production checkpoint and lock are never read or written by this function. Test event IDs
    have a TEST prefix and are deterministic for a given test_id/vehicle/interval. A send timeout
    can still cause replay, so downstream consumers should deduplicate by event_id.
    """
    checkpoint = read_cloud_checkpoint(checkpoint_blob)
    if checkpoint is None:
        checkpoint = {
            "version": 1,
            "checkpoint_type": "trackpulse_eventhub_test_producer",
            "test_id": test_id,
            "test_run_id": f"test-{test_id}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}Z",
            "historical_output": str(historical_dir),
            "historical_events": target_events,
            "historical_next_interval": historical_next_interval,
            "next_interval": historical_next_interval,
            "next_vehicle_index": 0,
            "emitted_events": 0,
            "total_intervals_est": max(total_intervals_est_default, historical_next_interval + 1),
            "states": copy.deepcopy(historical_checkpoint["states"]),
            "created_at_utc": utc_now_iso(),
            "updated_at_utc": utc_now_iso(),
        }
        validate_test_checkpoint(
            checkpoint,
            test_id=test_id,
            historical_dir=historical_dir,
            target_events=target_events,
            vehicles=vehicles,
            historical_next_interval=historical_next_interval,
        )
        persist_checkpoint(
            checkpoint_blob=checkpoint_blob,
            checkpoint_path=checkpoint_path,
            checkpoint=checkpoint,
            lease_guard=lease_guard,
        )
        LOGGER.info("Initialized isolated test checkpoint for test_id=%s.", test_id)
    else:
        validate_test_checkpoint(
            checkpoint,
            test_id=test_id,
            historical_dir=historical_dir,
            target_events=target_events,
            vehicles=vehicles,
            historical_next_interval=historical_next_interval,
        )
        # Cloud is authoritative. Refresh only the dedicated local test mirror.
        atomic_json(checkpoint_path, checkpoint)

    run_events_acknowledged = 0
    LOGGER.info(
        "TEST MODE: test_id=%s | cloud checkpoint=%s | local mirror=%s",
        test_id,
        checkpoint_blob.blob_name,
        checkpoint_path,
    )
    LOGGER.info(
        "TEST MODE starts at interval=%s vehicle_index=%s; this invocation is capped at %s new events.",
        checkpoint["next_interval"],
        checkpoint["next_vehicle_index"],
        max_events,
    )

    while run_events_acknowledged < max_events:
        lease_guard.assert_held()
        interval_index = int(checkpoint["next_interval"])
        next_vehicle_index = int(checkpoint["next_vehicle_index"])
        committed_states = checkpoint["states"]

        # Build only a bounded batch against a working copy. State is committed to the checkpoint
        # only after send_batch returns successfully.
        working_states = copy.deepcopy(committed_states)
        batch = producer.create_batch()
        staged_count = 0
        staged_next_vehicle_index = next_vehicle_index
        batch_event_ids: list[str] = []
        remaining_for_run = max_events - run_events_acknowledged
        target_batch_count = min(batch_size, remaining_for_run, vehicles - next_vehicle_index)

        while staged_count < target_batch_count:
            state_after_event = copy.deepcopy(working_states[staged_next_vehicle_index])
            event, _labels = generator.make_event(
                state_after_event,
                interval_index,
                start,
                interval_seconds,
                seed,
                int(checkpoint["total_intervals_est"]),
            )
            if not isinstance(event, dict):
                raise ValueError("Generator returned a non-object event in test mode.")
            original_event_id = event.get("event_id")
            if not isinstance(original_event_id, str) or not original_event_id.strip():
                raise ValueError("Generator returned an event without a valid event_id in test mode.")

            # Separate test IDs from production IDs; test mode must not appear as production data.
            event["event_id"] = f"TEST-{test_id}-{original_event_id}"
            event["test_mode"] = True
            event["test_id"] = test_id
            event_payload = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
            try:
                batch.add(EventData(event_payload))
            except ValueError as exc:
                if staged_count == 0:
                    raise ValueError(
                        f"A single test event ({event['event_id']}) exceeds the Event Hubs batch size limit."
                    ) from exc
                # The current event's state was kept in a separate object and is not staged; send
                # the previous events, persist them, then regenerate this event from checkpoint.
                break

            working_states[staged_next_vehicle_index] = state_after_event
            batch_event_ids.append(event["event_id"])
            staged_next_vehicle_index += 1
            staged_count += 1

        if staged_count == 0:
            raise RuntimeError("Test mode built an empty batch; refusing to spin without progress.")

        lease_guard.assert_held()
        try:
            producer.send_batch(batch)
        except Exception:
            LOGGER.exception(
                "Test batch send failed for test_id=%s. The checkpoint remains at the last confirmed batch; "
                "some events may have reached Event Hubs and may be replayed.",
                test_id,
            )
            raise

        # Successful send returned. Advance only by the exact number of events in this batch.
        updated_next_interval = interval_index
        updated_vehicle_index = staged_next_vehicle_index
        if updated_vehicle_index >= vehicles:
            updated_next_interval = interval_index + 1
            updated_vehicle_index = 0

        checkpoint = {
            **checkpoint,
            "next_interval": updated_next_interval,
            "next_vehicle_index": updated_vehicle_index,
            "emitted_events": int(checkpoint["emitted_events"]) + staged_count,
            "states": working_states,
            "updated_at_utc": utc_now_iso(),
            "last_acknowledged_batch_count": staged_count,
            "last_acknowledged_batch_last_event_id": batch_event_ids[-1],
        }
        validate_test_checkpoint(
            checkpoint,
            test_id=test_id,
            historical_dir=historical_dir,
            target_events=target_events,
            vehicles=vehicles,
            historical_next_interval=historical_next_interval,
        )
        persist_checkpoint(
            checkpoint_blob=checkpoint_blob,
            checkpoint_path=checkpoint_path,
            checkpoint=checkpoint,
            lease_guard=lease_guard,
        )
        run_events_acknowledged += staged_count
        LOGGER.info(
            "TEST batch acknowledged | batch_events=%s | this_run=%s/%s | total_test_events=%s | "
            "next_interval=%s | next_vehicle_index=%s | last_event_id=%s",
            staged_count,
            run_events_acknowledged,
            max_events,
            checkpoint["emitted_events"],
            checkpoint["next_interval"],
            checkpoint["next_vehicle_index"],
            batch_event_ids[-1],
        )
        if batch_pause_seconds and run_events_acknowledged < max_events:
            time.sleep(batch_pause_seconds)

    LOGGER.info(
        "TEST MODE completed: acknowledged %s new events in this invocation. Production checkpoint untouched.",
        run_events_acknowledged,
    )
    return run_events_acknowledged, int(checkpoint["next_interval"])


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
    batch_pause_seconds: float,
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
        if batch_pause_seconds:
            time.sleep(batch_pause_seconds)
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
        default=0.0,
        help="Optional EXTRA pause after each fleet tick. The live tick cadence is controlled by --stream-interval-seconds.",
    )
    parser.add_argument(
        "--stream-interval-seconds",
        type=int,
        default=1,
        help="Live telemetry cadence: generate one event per vehicle every N seconds. Default 1 means one fleet tick per second.",
    )
    parser.add_argument(
        "--checkpoint-every-intervals",
        type=int,
        default=30,
        help="Asynchronously persist the production checkpoint every N fully sent fleet ticks; Ctrl+C flushes the latest fully completed tick.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Maximum number of events per Event Hubs send batch; byte limits may flush earlier.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help=(
            "Maximum NEW events in this invocation; 0 means unlimited in production mode. "
            "Production mode requires 0 or a multiple of the vehicle count; --test-mode supports "
            "any positive limit with an isolated per-event checkpoint."
        ),
    )
    parser.add_argument(
        "--batch-pause-seconds",
        type=float,
        default=0.2,
        help="Pause after each acknowledged send batch to smooth ingress bursts; 0 disables pacing.",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Use a separate test checkpoint and lock; production checkpoint paths are never used.",
    )
    parser.add_argument(
        "--test-id",
        default="smoke",
        help="Isolated test namespace. Reuse it to resume the same test; use a new ID for a fresh test.",
    )
    parser.add_argument(
        "--transport",
        choices=("amqp", "websocket"),
        default="amqp",
        help="Event Hubs transport. websocket uses port 443; amqp uses port 5671.",
    )
    parser.add_argument(
        "--socket-timeout",
        type=float,
        default=10.0,
        help="Socket timeout in seconds; overrides the very short SDK default for raw AMQP.",
    )
    parser.add_argument(
        "--retry-total",
        type=int,
        default=5,
        help="SDK retry attempts for transient send failures.",
    )
    parser.add_argument(
        "--retry-backoff-factor",
        type=float,
        default=1.0,
        help="Exponential retry backoff factor.",
    )
    parser.add_argument(
        "--retry-backoff-max",
        type=float,
        default=30.0,
        help="Maximum retry backoff in seconds.",
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
    if args.stream_interval_seconds < 1:
        parser.error("--stream-interval-seconds must be >= 1.")
    if args.checkpoint_every_intervals < 1:
        parser.error("--checkpoint-every-intervals must be >= 1.")
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1.")
    if args.max_events < 0:
        parser.error("--max-events must be >= 0.")
    if args.batch_pause_seconds < 0:
        parser.error("--batch-pause-seconds must be >= 0.")
    if args.socket_timeout <= 0:
        parser.error("--socket-timeout must be > 0.")
    if args.retry_total < 0:
        parser.error("--retry-total must be >= 0.")
    if args.retry_backoff_factor < 0 or args.retry_backoff_max < 0:
        parser.error("Retry backoff values must be >= 0.")
    if args.test_mode:
        if args.max_events <= 0:
            parser.error("--test-mode requires --max-events with a positive value (e.g. 10 or 100).")
        if args.checkpoint is not None:
            parser.error("Do not pass --checkpoint with --test-mode; an isolated local mirror is selected automatically.")
        if not args.test_id or len(args.test_id) > 64 or any(
            ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for ch in args.test_id
        ) or args.test_id in (".", ".."):
            parser.error("--test-id must be 1-64 characters using only letters, digits, dot, underscore, or hyphen; '.' and '..' are invalid.")
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

    if args.test_mode:
        # Hard-separate test state from production even if production blob flags were supplied.
        test_suffix = args.test_id
        checkpoint_path = (historical_dir / f"stream_checkpoint.test-{test_suffix}.json").resolve()
        selected_cloud_checkpoint_blob = f"checkpoints/producer/tests/{test_suffix}/stream_checkpoint.json"
        selected_cloud_lock_blob = f"checkpoints/producer/tests/{test_suffix}/stream_checkpoint.lock"
        LOGGER.warning(
            "TEST MODE is enabled. Dedicated cloud blobs will be used; the production checkpoint and lock will not be touched."
        )
    else:
        checkpoint_path = (args.checkpoint or (historical_dir / "stream_checkpoint.json")).expanduser().resolve()
        selected_cloud_checkpoint_blob = args.cloud_checkpoint_blob
        selected_cloud_lock_blob = args.cloud_lock_blob
    if checkpoint_path in (manifest_path.resolve(), historical_checkpoint_path.resolve()):
        parser.error(
            "The local checkpoint mirror cannot overwrite manifest.json or the historical checkpoint.json."
        )
    if not args.test_mode and args.max_events and args.max_events % vehicles != 0:
        parser.error(
            f"--max-events must be 0 or a multiple of the vehicle count ({vehicles:,}) in production mode; "
            "use --test-mode for isolated partial-interval tests."
        )

    LOGGER.info("Validated historical data: %s events, %s vehicles, %s-second intervals.",
                f"{target_events:,}", f"{vehicles:,}", interval_seconds)
    LOGGER.info("Historical end boundary: next_interval=%s.", historical_next_interval)
    LOGGER.info("Historical checkpoint is read-only: %s", historical_checkpoint_path)
    LOGGER.info("Local checkpoint mirror: %s", checkpoint_path)
    LOGGER.info("Batch size=%s, batch pause=%.2fs, transport=%s, socket timeout=%.1fs.", args.batch_size, args.batch_pause_seconds, args.transport, args.socket_timeout)

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
    checkpoint_executor: ThreadPoolExecutor | None = None
    latest_checkpoint_future: Future | None = None
    checkpoint_futures: list[Future] = []
    last_checkpoint_submitted_next_interval: int | None = None
    next_interval: int | None = None
    states: list[dict[str, Any]] | None = None
    cloud_checkpoint: dict[str, Any] | None = None
    checkpoint_audit_fields: dict[str, Any] = {}
    run_events_acknowledged = 0
    in_flight_interval = False
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
            blob=selected_cloud_checkpoint_blob,
        )
        lock_blob = blob_service.get_blob_client(
            container=args.checkpoint_container,
            blob=selected_cloud_lock_blob,
        )

        lease_guard = CheckpointLease(lock_blob)
        try:
            lease_guard.acquire()
        except Exception as exc:
            raise RuntimeError(
                "Could not acquire the producer lock. Another producer may already be running, "
                "or the Storage connection lacks blob lease permissions. Do not start a second producer."
            ) from exc

        if args.test_mode:
            transport_type = (
                TransportType.Amqp if args.transport == "amqp" else TransportType.AmqpOverWebsocket
            )
            producer = EventHubProducerClient.from_connection_string(
                conn_str=eventhub_connection,
                eventhub_name=eventhub_name,
                transport_type=transport_type,
                socket_timeout=args.socket_timeout,
                idle_timeout=120,
                retry_total=args.retry_total,
                retry_backoff_factor=args.retry_backoff_factor,
                retry_backoff_max=args.retry_backoff_max,
            )
            LOGGER.info(
                "Event Hubs client configured: transport=%s, socket_timeout=%.1fs, retries=%s.",
                args.transport,
                args.socket_timeout,
                args.retry_total,
            )
            run_events_acknowledged, next_interval = run_isolated_test_mode(
                checkpoint_blob=checkpoint_blob,
                checkpoint_path=checkpoint_path,
                producer=producer,
                generator=generator_module,
                historical_dir=historical_dir,
                historical_checkpoint=historical_checkpoint,
                target_events=target_events,
                vehicles=vehicles,
                interval_seconds=interval_seconds,
                start=start,
                seed=seed,
                historical_next_interval=historical_next_interval,
                total_intervals_est_default=max(
                    historical_next_interval + DEFAULT_SIMULATION_HORIZON_EXTRA_INTERVALS, 1
                ),
                test_id=args.test_id,
                max_events=args.max_events,
                batch_size=args.batch_size,
                batch_pause_seconds=args.batch_pause_seconds,
                lease_guard=lease_guard,
            )
            LOGGER.info("TEST MODE finished; production checkpoint was not accessed.")
        else:
            cloud_checkpoint = read_cloud_checkpoint(checkpoint_blob)
            if cloud_checkpoint is None:
                cloud_checkpoint = create_initial_checkpoint(
                    historical_dir=historical_dir,
                    target_events=target_events,
                    historical_next_interval=historical_next_interval,
                    vehicles=vehicles,
                    states=historical_checkpoint["states"],
                    stream_interval_seconds=args.stream_interval_seconds,
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
                    stream_interval_seconds=args.stream_interval_seconds,
                )
                # Migrate v1/v2 checkpoints or normalize the local mirror before publishing anything.
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
            last_checkpoint_submitted_next_interval = next_interval
            historical_end = start + timedelta(seconds=(historical_next_interval - 1) * interval_seconds)
            # Keep source event IDs aligned to historical interval indexes, but advance live timestamps
            # in real one-second steps beginning immediately after the historical end boundary.
            live_generator_start = (
                historical_end
                + timedelta(seconds=args.stream_interval_seconds)
                - timedelta(seconds=historical_next_interval * args.stream_interval_seconds)
            )
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
            LOGGER.info(
                "Live cadence=%ss per fleet tick (target %s events/s for %s vehicles); cloud checkpoint every %s ticks.",
                args.stream_interval_seconds,
                f"{vehicles / args.stream_interval_seconds:,.0f}",
                vehicles,
                args.checkpoint_every_intervals,
            )
            LOGGER.info("Historical end boundary: %s; first live event timestamp: %s.",
                        historical_end.isoformat().replace("+00:00", "Z"),
                        (historical_end + timedelta(seconds=args.stream_interval_seconds)).isoformat().replace("+00:00", "Z"))
            LOGGER.info("Configured max NEW events this run: %s",
                        f"{args.max_events:,}" if args.max_events else "unlimited (Ctrl+C to stop)")
            LOGGER.info(
                "Delivery is at-least-once across crashes; deduplicate downstream by event_id."
            )

            transport_type = (
                TransportType.Amqp if args.transport == "amqp" else TransportType.AmqpOverWebsocket
            )
            producer = EventHubProducerClient.from_connection_string(
                conn_str=eventhub_connection,
                eventhub_name=eventhub_name,
                transport_type=transport_type,
                socket_timeout=args.socket_timeout,
                idle_timeout=120,
                retry_total=args.retry_total,
                retry_backoff_factor=args.retry_backoff_factor,
                retry_backoff_max=args.retry_backoff_max,
            )
            LOGGER.info(
                "Event Hubs client configured: transport=%s, socket_timeout=%.1fs, retries=%s.",
                args.transport,
                args.socket_timeout,
                args.retry_total,
            )

            # Checkpoints are large (~5,000 vehicle states), so remote writes are serialized in a
            # background worker instead of pausing the one-second telemetry cadence.
            checkpoint_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="trackpulse-checkpoint")
            completed_since_checkpoint = 0

            while True:
                lease_guard.assert_held()
                # Surface completed checkpoint failures before sending more events.
                for pending_checkpoint in list(checkpoint_futures):
                    if pending_checkpoint.done():
                        pending_checkpoint.result()
                        checkpoint_futures.remove(pending_checkpoint)
                if args.max_events and run_events_acknowledged >= args.max_events:
                    break

                tick_started = time.monotonic()
                interval_index = next_interval
                # Work on a copy. If Ctrl+C interrupts a partially sent fleet tick, the state for
                # that incomplete tick is discarded and the authoritative checkpoint remains at
                # the last fully completed tick. Already-accepted events can replay by stable event_id.
                working_states = copy.deepcopy(states)
                in_flight_interval = True
                acknowledged = send_fleet_interval(
                    producer=producer,
                    generator=generator_module,
                    states=working_states,
                    interval_index=interval_index,
                    start=live_generator_start,
                    interval_seconds=args.stream_interval_seconds,
                    seed=seed,
                    total_intervals_est=total_intervals_est,
                    batch_size=args.batch_size,
                    batch_pause_seconds=args.batch_pause_seconds,
                    lease_guard=lease_guard,
                )
                if acknowledged != vehicles:
                    raise RuntimeError(
                        f"Interval {interval_index} acknowledged {acknowledged} events; expected {vehicles}."
                    )

                # The whole fleet tick has been acknowledged: now adopt its updated state.
                states = working_states
                next_interval += 1
                run_events_acknowledged += acknowledged
                emitted_events_total = (next_interval - historical_next_interval) * vehicles
                in_flight_interval = False
                completed_since_checkpoint += 1

                cloud_checkpoint = {
                    "version": CHECKPOINT_VERSION,
                    "checkpoint_type": "trackpulse_eventhub_producer",
                    "historical_output": str(historical_dir),
                    "historical_events": target_events,
                    "historical_next_interval": historical_next_interval,
                    "stream_interval_seconds": args.stream_interval_seconds,
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
                cloud_checkpoint.update(checkpoint_audit_fields)

                if completed_since_checkpoint >= args.checkpoint_every_intervals:
                    snapshot = copy.deepcopy(cloud_checkpoint)
                    assert checkpoint_executor is not None
                    latest_checkpoint_future = checkpoint_executor.submit(
                        persist_checkpoint,
                        checkpoint_blob=checkpoint_blob,
                        checkpoint_path=checkpoint_path,
                        checkpoint=snapshot,
                        lease_guard=lease_guard,
                    )
                    checkpoint_futures.append(latest_checkpoint_future)
                    last_checkpoint_submitted_next_interval = next_interval
                    completed_since_checkpoint = 0
                    LOGGER.info(
                        "Checkpoint write queued | next_interval=%s | total committed events=%s | cloud write runs in background.",
                        next_interval,
                        f"{emitted_events_total:,}",
                    )

                send_elapsed = time.monotonic() - tick_started
                observed_rate = acknowledged / send_elapsed if send_elapsed > 0 else float("inf")
                LOGGER.info(
                    "Fleet tick acknowledged | interval=%s | events=%s | send_elapsed=%.3fs | send_rate=%.0f events/s | total_this_run=%s | next_interval=%s",
                    interval_index,
                    f"{acknowledged:,}",
                    send_elapsed,
                    observed_rate,
                    f"{run_events_acknowledged:,}",
                    next_interval,
                )
                if send_elapsed > args.stream_interval_seconds:
                    LOGGER.warning(
                        "Producer took %.3fs to send a %s-event fleet tick; actual real-time rate is below the %s events/s target. Check Event Hubs throttling, network, CPU, and batch settings.",
                        send_elapsed,
                        f"{vehicles:,}",
                        f"{vehicles / args.stream_interval_seconds:,.0f}",
                    )

                if args.max_events and run_events_acknowledged >= args.max_events:
                    break
                # Maintain a wall-clock cadence. If the send itself exceeds the interval, do not
                # add another delay; log the rate shortfall and continue as fast as possible.
                if args.sleep_seconds:
                    time.sleep(args.sleep_seconds)
                remaining = args.stream_interval_seconds - (time.monotonic() - tick_started)
                if remaining > 0:
                    time.sleep(remaining)

            LOGGER.info("Producer run completed normally.")

    except KeyboardInterrupt:
        return_code = 130
        LOGGER.warning(
            "Interrupted by user. Will flush the latest fully completed fleet tick to the production checkpoint. "
            "If interruption happened mid-tick, that incomplete tick is not checkpointed; accepted events from it "
            "may be replayed after restart and should be deduplicated by event_id."
        )
    except Exception as exc:
        return_code = 1
        LOGGER.exception(
            "Producer stopped with an error. The checkpoint will be advanced only through fully completed fleet ticks. "
            "Events already accepted from an incomplete tick may be replayed after restart; deduplicate by event_id. "
            "Cause: %s",
            exc,
        )
    finally:
        # Graceful stop/failure: submit the latest fully completed state, then wait for serialized
        # checkpoint writes before releasing the Blob lease. If the stop occurred mid-tick,
        # `states` still represents only the preceding completed tick because ticks use a working copy.
        if (
            not args.test_mode
            and checkpoint_executor is not None
            and lease_guard is not None
            and cloud_checkpoint is not None
            and next_interval is not None
            and states is not None
            and last_checkpoint_submitted_next_interval is not None
        ):
            try:
                # Always flush the latest completed state when stopping, even if the periodic
                # checkpoint future had failed or was already queued at the same interval.
                final_snapshot = copy.deepcopy(cloud_checkpoint)
                latest_checkpoint_future = checkpoint_executor.submit(
                    persist_checkpoint,
                    checkpoint_blob=checkpoint_blob,
                    checkpoint_path=checkpoint_path,
                    checkpoint=final_snapshot,
                    lease_guard=lease_guard,
                )
                checkpoint_futures.append(latest_checkpoint_future)
                last_checkpoint_submitted_next_interval = next_interval
                LOGGER.info(
                    "Final checkpoint write queued | next_interval=%s | committed streaming events=%s.",
                    next_interval,
                    f"{int(cloud_checkpoint.get('emitted_events', 0)):,}",
                )
                if latest_checkpoint_future is not None:
                    latest_checkpoint_future.result()
                LOGGER.info(
                    "Cloud checkpoint confirmed at next_interval=%s.",
                    last_checkpoint_submitted_next_interval,
                )
            except Exception as exc:  # preserve stop reporting but make checkpoint issue visible
                LOGGER.error("Could not confirm final cloud checkpoint write: %s", exc)
            finally:
                checkpoint_executor.shutdown(wait=True, cancel_futures=False)
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

    if not args.test_mode:
        LOGGER.info("Events in fully committed intervals this run: %s", f"{run_events_acknowledged:,}")
    if return_code == 0:
        LOGGER.info("Next interval to resume: %s", locals().get("next_interval", "unchanged"))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())

