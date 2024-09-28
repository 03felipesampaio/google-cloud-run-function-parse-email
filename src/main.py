from dotenv import load_dotenv
load_dotenv()

from google.cloud import storage, bigquery
import functions_framework
from cloudevents.http.event import CloudEvent

import uber_scraper
import json
from base64 import urlsafe_b64decode

import os


@functions_framework.cloud_event
def parse_email_from_file(cloud_event: CloudEvent):
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
    bucket = storage_client.get_bucket(cloud_event.data["bucket"])
    filename = cloud_event.data["name"]

    if filename.startswith("Uber/Recibos/") and filename.endswith(".json"):
        # Download json file
        file_content = json.loads(bucket.blob(filename).download_as_text())
        uber_receipt = uber_scraper.parse_message_body(
            urlsafe_b64decode(file_content["payload"]["body"]["data"])
        )
        # Datetime.date is not json serializable. So all dates and timestamps must be converted
        uber_receipt['date'] = str(uber_receipt['date'])

        bigquery_dataset = bigquery_client.create_dataset(os.environ["BIGQUERY_DATASET_NAME"], exists_ok=True)
        bigquery_table = bigquery_client.create_table(os.environ['UBER_RECEIPTS_BIGQUERY_TABLE'], exists_ok=True)

        errors = bigquery_client.insert_rows_json(bigquery_table, [uber_receipt])
        
        if errors:
            print("Encountered errors while inserting rows: {}".format(errors))