#!/usr/bin/env bash

set -euxo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "Creating bucket $BUCKET..."
gcloud storage buckets create "gs://$BUCKET"

echo "Creating dataset $DATASET..."
bq mk "$DATASET"

echo "Creating table $TABLE..."
bq mk -t "$DATASET.$TABLE" ingestion_id:string,date:date,project_id:string,currency_symbol:string,number_of_transactions:integer,currency_value_usd:string

echo "Seeding prices..."

gcloud storage cp "$SCRIPT_DIR/prices.csv" "gs://$BUCKET/2024/4/1/prices.csv"
gcloud storage cp "$SCRIPT_DIR/prices.csv" "gs://$BUCKET/2024/4/2/prices.csv"
gcloud storage cp "$SCRIPT_DIR/prices.csv" "gs://$BUCKET/2024/4/15/prices.csv"
gcloud storage cp "$SCRIPT_DIR/prices.csv" "gs://$BUCKET/2024/4/16/prices.csv"

echo "Seeding sample data..."
gcloud storage cp "$SCRIPT_DIR/sample_data.csv" "gs://$BUCKET/sample_data.csv"
