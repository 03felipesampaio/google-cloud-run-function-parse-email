#!/bin/bash

# Load the environment variables
source .env

# Deploy the function
gcloud functions deploy parse_emails \
    --gen2 \
    --region=$REGION \
    --runtime=python312 \
    --entry-point=parse_email_from_file \
    --source=src \
    --trigger-bucket=$BUCKET_NAME \
    # --set-env-vars=BIGQUERY_DATASET_NAME=uber_receipts,BIGQUERY_TABLE_NAME=uber_receipts_table 