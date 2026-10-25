import os
import sys
import time
import hashlib
import csv
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError, ResourceNotFoundError

# ---------------- Configuration ----------------

CONTAINER_NAME = "trackpulse-data"
MAX_ATTEMPTS = 3

ROOT = Path(__file__).resolve().parent.parent
HISTORICAL = ROOT / "data" / "historical_azure"
MASTER = ROOT / "data" / "master_data"

UPLOADS = [
    (HISTORICAL / "telemetry", "historical/telemetry"),
    (HISTORICAL / "anomaly_chunks", "historical/anomaly_labels"),
    (HISTORICAL / "trips.json", "historical/trips"),
    (HISTORICAL / "manifest.json", "historical/metadata"),
    (MASTER, "master_data"),
]

REPORT = ROOT / "upload_report.csv"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_files(source: Path):
    if not source.exists():
        raise FileNotFoundError(f"Missing source: {source}")

    if source.is_file():
        yield source, source.name
    else:
        for path in sorted(source.rglob("*")):
            if path.is_file():
                yield path, path.relative_to(source).as_posix()


def main():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        sys.exit("ERROR: AZURE_STORAGE_CONNECTION_STRING is not set.")

    for source, _ in UPLOADS:
        if not source.exists():
            sys.exit(f"ERROR: Required source does not exist: {source}")

    service = BlobServiceClient.from_connection_string(
        connection_string,
        retry_total=3,
    )
    container = service.get_container_client(CONTAINER_NAME)

    # Verify container access before uploading.
    container.get_container_properties()
    print(f"Connected to container: {CONTAINER_NAME}")

    report_rows = []
    uploaded = skipped = failed = 0
    total_bytes = 0

    for source, prefix in UPLOADS:
        files = list(iter_files(source))
        print(f"\n{source} -> {prefix} ({len(files)} files)")

        for local_file, relative_name in files:
            blob_name = f"{prefix}/{relative_name}".replace("\\", "/")
            blob = container.get_blob_client(blob_name)
            size = local_file.stat().st_size
            digest = sha256_file(local_file)

            try:
                try:
                    props = blob.get_blob_properties()
                    metadata = props.metadata or {}

                    if (
                        props.size == size
                        and metadata.get("source_sha256") == digest
                    ):
                        print(f"SKIP verified: {blob_name}")
                        skipped += 1
                        report_rows.append(
                            [str(local_file), blob_name, size, "SKIPPED_VERIFIED", digest]
                        )
                        continue

                    if props.size == size and not metadata.get("source_sha256"):
                        print(
                            f"REUPLOAD to verify: {blob_name} "
                            "(existing blob has no source hash)"
                        )

                except ResourceNotFoundError:
                    pass

                last_error = None

                for attempt in range(1, MAX_ATTEMPTS + 1):
                    try:
                        with local_file.open("rb") as data:
                            blob.upload_blob(
                                data,
                                overwrite=True,
                                metadata={"source_sha256": digest},
                                max_concurrency=2,
                            )

                        props = blob.get_blob_properties()
                        if props.size != size:
                            raise IOError(
                                f"Size mismatch: local={size}, remote={props.size}"
                            )

                        if (props.metadata or {}).get("source_sha256") != digest:
                            raise IOError("Remote SHA-256 metadata verification failed")

                        last_error = None
                        break

                    except (AzureError, OSError) as exc:
                        last_error = exc
                        print(
                            f"Attempt {attempt}/{MAX_ATTEMPTS} failed: "
                            f"{blob_name}: {exc}"
                        )
                        if attempt < MAX_ATTEMPTS:
                            time.sleep(2 ** attempt)

                if last_error:
                    raise last_error

                uploaded += 1
                total_bytes += size
                print(f"UPLOADED: {blob_name} ({size:,} bytes)")
                report_rows.append(
                    [str(local_file), blob_name, size, "UPLOADED_VERIFIED", digest]
                )

            except Exception as exc:
                failed += 1
                print(f"FAILED: {blob_name}: {exc}")
                report_rows.append(
                    [str(local_file), blob_name, size, f"FAILED: {exc}", digest]
                )

    with REPORT.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            ["local_path", "blob_name", "size_bytes", "status", "sha256"]
        )
        writer.writerows(report_rows)

    print("\n========== UPLOAD SUMMARY ==========")
    print(f"Uploaded and verified: {uploaded}")
    print(f"Skipped (hash verified): {skipped}")
    print(f"Failed: {failed}")
    print(f"Bytes uploaded this run: {total_bytes:,}")
    print(f"Report: {REPORT}")

    if failed:
        sys.exit(1)

    print("All files processed successfully.")


if __name__ == "__main__":
    main()
