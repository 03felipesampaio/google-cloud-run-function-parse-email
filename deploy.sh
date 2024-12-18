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
    --set-env-vars=BIGQUERY_DATASET_NAME=$BIGQUERY_DATASET_NAME,UBER_RECEIPTS_BIGQUERY_TABLE=$UBER_RECEIPTS_BIGQUERY_TABLE,BRONZE_FINANCE_BUCKET=$BRONZE_FINANCE_BUCKET,INTER_PASSWORD=$INTER_PASSWORD,BANK_STATEMENT_PARSER_URL=$BANK_STATEMENT_PARSER_URL