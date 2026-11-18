#!/bin/bash

JSON_FILE="${1:-resources.json}"

if [ ! -f "$JSON_FILE" ]; then
    echo "[ERROR] File '$JSON_FILE' not found!"
    exit 1
fi

echo "[INFO] Copying $JSON_FILE into the Airflow container..."
MSYS_NO_PATHCONV=1 docker cp "$JSON_FILE" webserver:/opt/airflow/variables.json

echo "[INFO] Importing variables into Airflow..."
MSYS_NO_PATHCONV=1 docker compose exec -T webserver airflow variables import variables.json

echo "[SUCCESS] Variables applied successfully!"