import os
import sys

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError


def main():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if not connection_string:
        sys.exit(
            "ERROR: AZURE_STORAGE_CONNECTION_STRING is not set."
        )

    try:
        service = BlobServiceClient.from_connection_string(
            connection_string
        )

        container = service.get_container_client("trackpulse-data")

        properties = container.get_container_properties()

        print("Azure connection successful!")
        print(f"Container: {container.container_name}")
        print(f"Last modified: {properties['last_modified']}")
        print("Ready for upload.")

    except AzureError as exc:
        print(f"Azure connection failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
