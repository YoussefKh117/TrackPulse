import os
from pathlib import Path

from azure.storage.blob import BlobServiceClient

project_root = Path(__file__).resolve().parent.parent
local_file = project_root / "data" / "historical_azure" / "manifest.json"

container_name = "trackpulse-data"
blob_name = "historical/metadata/manifest.json"

connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

if not connection_string:
    raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set")

if not local_file.is_file():
    raise FileNotFoundError(f"File not found: {local_file}")

service = BlobServiceClient.from_connection_string(connection_string)
container = service.get_container_client(container_name)
blob = container.get_blob_client(blob_name)

with local_file.open("rb") as data:
    blob.upload_blob(data, overwrite=True)

properties = blob.get_blob_properties()

print("Upload successful!")
print(f"Blob: {blob_name}")
print(f"Size: {properties.size:,} bytes")
