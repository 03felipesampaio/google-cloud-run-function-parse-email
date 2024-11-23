from dotenv import load_dotenv

load_dotenv()

from .input_file import InputFile
from google.cloud import storage, bigquery
import functions_framework
from cloudevents.http.event import CloudEvent

import uber_scraper
from handlers import expenses_trackers
import json
from base64 import urlsafe_b64decode

import os
import io

from loguru import logger


def read_email(file_content: io.BytesIO) -> dict:
    json_content = json.loads(file_content)

    # Decodes the email body from base64
    json_content["payload"]["body"]["data"] = urlsafe_b64decode(
        json_content["payload"]["body"]["data"]
    ).decode("utf-8")

    return json_content


@functions_framework.cloud_event
def parse_email_from_file(cloud_event: CloudEvent):
    storage_client = storage.Client()
    
    logger.info(f"{cloud_event}")

    input_file = InputFile(
        storage_client,
        cloud_event.data["bucket"],
        cloud_event.data["name"],
        cloud_event.data["contentType"],
    )

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
        uber_receipt["ride_date"] = str(uber_receipt["ride_date"])

        bigquery_dataset = bigquery_client.create_dataset(
            os.environ["BIGQUERY_DATASET_NAME"], exists_ok=True
        )
        bigquery_table = bigquery.Table(
            os.environ["UBER_RECEIPTS_BIGQUERY_TABLE"],
            schema=(
                bigquery.SchemaField(
                    "ride_date", "DATE", mode="REQUIRED", description="Date of the ride"
                ),
                bigquery.SchemaField(
                    "ride_value",
                    "NUMERIC",
                    mode="REQUIRED",
                    description="Value of the ride",
                    precision=10,
                    scale=2,
                ),
                bigquery.SchemaField(
                    "stars",
                    "NUMERIC",
                    mode="REQUIRED",
                    description="Driver rating",
                    precision=3,
                    scale=2,
                ),
                bigquery.SchemaField(
                    "category",
                    "STRING",
                    mode="REQUIRED",
                    description="Ride category (UberX, Uber Black, etc)",
                ),
                bigquery.SchemaField(
                    "distance",
                    "NUMERIC",
                    mode="REQUIRED",
                    description="Ride distance (in kilometers)",
                    precision=5,
                    scale=2,
                ),
                bigquery.SchemaField(
                    "duration",
                    "NUMERIC",
                    mode="REQUIRED",
                    description="Ride duration (in minutes)",
                    precision=5,
                    scale=2,
                ),
            ),
        )
        bigquery_table = bigquery_client.create_table(bigquery_table, exists_ok=True)

        errors = bigquery_client.insert_rows_json(bigquery_table, [uber_receipt])

        if errors:
            print("Encountered errors while inserting rows: {}".format(errors))
        else:
            print("Inserted:", uber_receipt)
    if filename.startswith("Faturas/") and filename.endswith(".pdf"):
        bank_name = input_file.name.split("/")[1]
        logger.info(f"Parsing bill on file '{filename}' with content type '{input_file.content_type}'")
        
        try:
            output_file = expenses_trackers.parse_bill(bank_name.lower(), input_file)
        except Exception as e:
            logger.error(f"Failed to parse bill '{filename}'. Error: {e}")
            raise e

        bucket = storage_client.get_bucket(os.getenv("BRONZE_FINANCE_BUCKET"))
        
        
        blob_output_file = bucket.blob(
            f'bills/{input_file.name.replace(".pdf", ".parquet").replace("Faturas/", "").lower()}'
        )
        
        if blob_output_file.exists():
            logger.info(f"Overwriting file '{blob_output_file.name}' at bucket '{blob_output_file.bucket.name}'")
        else:
            logger.info(f"Creating file '{blob_output_file.name}' at bucket '{blob_output_file.bucket.name}'")
        
        blob_output_file.upload_from_string(
            output_file.content, content_type=output_file.headers["content-type"]
        )

        logger.info(f"Sent file '{blob_output_file.name}' to bucket '{blob_output_file.bucket.name}'")
        
    if filename.startswith("Extratos") and filename.endswith(".ofx"):
        bank_name = input_file.name.split("/")[1]
        
        logger.info(f"Parsing statement on file '{filename}' with content type '{input_file.content_type}'")
        
        try:
            output_file = expenses_trackers.parse_statement(bank_name.lower(), input_file)
        except Exception as e:
            logger.error(f"Failed to parse statement '{filename}'. Error: {e}")
            raise e

        bucket = storage_client.get_bucket(os.getenv("BRONZE_FINANCE_BUCKET"))
        
        blob_output_file = bucket.blob(
            f'statements/{input_file.name.replace(".ofx", ".parquet").replace("Extratos/", "").lower()}'
        )
        
        if blob_output_file.exists():
            logger.info(f"Overwriting file '{blob_output_file.name}' at bucket '{blob_output_file.bucket.name}'")
        else:
            logger.info(f"Creating file '{blob_output_file.name}' at bucket '{blob_output_file.bucket.name}'")
        
        blob_output_file.upload_from_string(
            output_file.content, content_type=output_file.headers["content-type"]
        )

        logger.info(f"Sent file '{blob_output_file.name}' to bucket '{blob_output_file.bucket.name}'")
    
