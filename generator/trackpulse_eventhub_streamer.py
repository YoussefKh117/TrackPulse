from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from azure.eventhub import EventData, EventHubProducerClient


def load_generator(path: Path):
    spec = importlib.util.spec_from_file_location("trackpulse_historical_generator", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load generator module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def atomic_json(path: Path, obj: dict[str, Any]) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    temp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Continue TrackPulse telemetry after historical data and send new events to Azure Event Hubs."
    )
    parser.add_argument("--historical-output", type=Path, required=True)
    parser.add_argument("--generator", type=Path, required=True,
                        help="Path to generate_historical_data_trackpulse.py")
    parser.add_argument("--sleep-seconds", type=float, default=1.0,
                        help="Wall-clock pause after each fleet batch; 0 means send as fast as possible.")
    parser.add_argument("--batch-size", type=int, default=5000,
                        help="Events per send batch. Use 5000 to send one full fleet interval at a time.")
    parser.add_argument("--max-events", type=int, default=0,
                        help="Stop after this many new events; 0 means run until interrupted.")
    parser.add_argument("--checkpoint", type=Path, default=None)
    args = parser.parse_args()

    if args.sleep_seconds < 0 or args.batch_size < 1 or args.max_events < 0:
        parser.error("sleep-seconds and max-events must be >= 0; batch-size must be >= 1")

    connection = os.getenv("EVENT_HUB_CONNECTION_STR")
    hub_name = os.getenv("EVENT_HUB_NAME")
    if not connection or not hub_name:
        parser.error("Set EVENT_HUB_CONNECTION_STR and EVENT_HUB_NAME environment variables first.")

    hist_dir = args.historical_output.resolve()
    manifest_path = hist_dir / "manifest.json"
    historical_checkpoint_path = hist_dir / "checkpoint.json"
    if not manifest_path.exists() or not historical_checkpoint_path.exists():
        parser.error("Historical manifest/checkpoint not found. Generate historical data first.")

    manifest = read_json(manifest_path)
    checkpoint = read_json(historical_checkpoint_path)
    if manifest.get("status") != "completed":
        parser.error(f"Historical generation is not completed (status={manifest.get('status')!r}).")
    config = manifest.get("config", {})
    vehicles = int(config.get("vehicles", 0))
    target_events = int(config.get("target_events", 0))
    interval_seconds = int(config.get("interval_seconds", 0))
    if vehicles < 1 or interval_seconds < 1:
        parser.error("Historical manifest has invalid vehicles/interval_seconds.")
    if int(manifest.get("events", -1)) != target_events:
        parser.error("Manifest event count does not equal configured target_events.")
    next_interval = int(checkpoint.get("next_interval", -1))
    if next_interval != target_events // vehicles:
        parser.error(
            f"Checkpoint is not at the end of history: next_interval={next_interval}, "
            f"expected={target_events // vehicles}."
        )
    states = checkpoint.get("states")
    if not isinstance(states, list) or len(states) != vehicles:
        parser.error("Historical checkpoint does not contain all vehicle states.")

    start_text = config.get("start_utc")
    if not start_text:
        parser.error("Historical manifest does not contain start_utc.")
    start = datetime.fromisoformat(start_text.replace("Z", "+00:00")).astimezone(timezone.utc)
    seed = int(config.get("seed", 42))
    total_intervals_est = max(next_interval + 100000, 1)
    generator = load_generator(args.generator.resolve())

    checkpoint_path = (args.checkpoint or (hist_dir / "stream_checkpoint.json")).resolve()
    stream_state = None
    if checkpoint_path.exists():
        stream_state = read_json(checkpoint_path)
        if stream_state.get("historical_output") != str(hist_dir):
            parser.error("Streaming checkpoint belongs to a different historical output.")
        if stream_state.get("historical_events") != target_events:
            parser.error("Streaming checkpoint historical event count does not match.")
        states = stream_state["states"]
        next_interval = int(stream_state["next_interval"])
        emitted_events = int(stream_state.get("emitted_events", 0))
    else:
        emitted_events = 0
        atomic_json(checkpoint_path, {
            "version": 1,
            "historical_output": str(hist_dir),
            "historical_events": target_events,
            "next_interval": next_interval,
            "emitted_events": 0,
            "states": states,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        })

    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection,
        eventhub_name=hub_name,
    )
    print(f"Historical events: {target_events:,}")
    print(f"Continuing from interval {next_interval}; first new event index is {next_interval}.")
    print(f"Streaming checkpoint: {checkpoint_path}")
    print("Press Ctrl+C to stop. Restarting will continue from stream_checkpoint.json.")

    try:
        with producer:
            while True:
                if args.max_events and emitted_events >= args.max_events:
                    break

                # Snapshot the interval's starting state. The durable checkpoint advances
                # only after every vehicle event for this interval has been acknowledged.
                interval_state_before = json.loads(json.dumps(states))
                interval_events_sent = 0
                batch = producer.create_batch()

                for p in states:
                    if args.max_events and emitted_events + interval_events_sent >= args.max_events:
                        break
                    event, _labels = generator.make_event(
                        p, next_interval, start, interval_seconds, seed, total_intervals_est
                    )
                    payload = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
                    try:
                        batch.add(EventData(payload))
                    except ValueError:
                        if len(batch) == 0:
                            raise RuntimeError(
                                "A single telemetry event is larger than the Event Hubs batch limit."
                            )
                        producer.send_batch(batch)
                        interval_events_sent += len(batch)
                        batch = producer.create_batch()
                        batch.add(EventData(payload))

                if len(batch):
                    producer.send_batch(batch)
                    interval_events_sent += len(batch)

                emitted_events += interval_events_sent
                if interval_events_sent == vehicles:
                    next_interval += 1
                    # Save updated state only after all events for the interval were sent.
                    atomic_json(checkpoint_path, {
                        "version": 1, "historical_output": str(hist_dir),
                        "historical_events": target_events, "next_interval": next_interval,
                        "emitted_events": emitted_events, "states": states,
                        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                    })
                else:
                    # A max-events cutoff in the middle of a fleet interval is intentionally
                    # not checkpointed as complete; restore state to the interval boundary.
                    states = interval_state_before
                    atomic_json(checkpoint_path, {
                        "version": 1, "historical_output": str(hist_dir),
                        "historical_events": target_events, "next_interval": next_interval,
                        "emitted_events": emitted_events, "states": states,
                        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                        "warning": "Partial fleet interval may be resent on restart; deduplicate by event_id.",
                    })
                if args.max_events and emitted_events >= args.max_events:
                    break
                if args.sleep_seconds:
                    time.sleep(args.sleep_seconds)
    except KeyboardInterrupt:
        print("\nStopped by user. Streaming checkpoint saved.")
    finally:
        producer.close()

    print(f"New events sent in this run/session total: {emitted_events:,}")
    print(f"Next interval to resume: {next_interval}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
