from google.cloud import storage
import functions_framework
from cloudevents.http.event import CloudEvent

import uber_scraper
import json
from base64 import urlsafe_b64decode


@functions_framework.cloud_event
def parse_email_from_file(cloud_event: CloudEvent):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(cloud_event.data["bucket"])
    filename = cloud_event.data["name"]

    if filename.startswith("Uber/Recibos/") and filename.endswith(".json"):
        # Download json file
        file_content = json.loads(bucket.blob(filename).download_as_text())
        uber_receipt = uber_scraper.parse_message_body(
            urlsafe_b64decode(file_content["payload"]["body"]["data"])
        )
