# Import necessary libraries
from google.cloud import bigquery
from datetime import datetime, timedelta

# --- Configuration ---
PROJECT_ID = "patrizia-s2s-bigquery"
GA4_DATASET_ID = "analytics_277316518"

# Define the expiration period in days
# Each events_YYYYMMDD table will expire 732 days after its YYYYMMDD date
EXPIRATION_DAYS = 732

# --- BigQuery Client Initialization ---
client = bigquery.Client(project=PROJECT_ID)

# Construct the full dataset ID
dataset_id = f"{PROJECT_ID}.{GA4_DATASET_ID}"

print(f"Starting process to set expiration for tables in dataset: {dataset_id}\n")

# Dynamically generate the list of tables for all of 2024
tables = []
start_date = datetime(2023, 9, 1)
end_date = datetime(2023, 9, 30)
current_date = start_date

while current_date <= end_date:
    table_name = f'events_{current_date.strftime("%Y%m%d")}'
    tables.append(table_name)
    current_date += timedelta(days=1)

# The rest of the script remains the same and will now
# process the dynamically generated list of tables.

for table_name in tables:
    # --- List and Process Tables ---
    date_str = table_name.replace("events_", "")

    table_date = datetime.strptime(date_str, "%Y%m%d").date()
    print(table_date)

    expiration_datetime = datetime(
      table_date.year, table_date.month, table_date.day
    ) + timedelta(days=EXPIRATION_DAYS)

    print(expiration_datetime)

    expiration_timestamp_ms = expiration_datetime
    print(expiration_timestamp_ms)

    # Get the full table reference
    table_ref = client.dataset(GA4_DATASET_ID).table(table_name)
    # Fetch the current table object to modify its properties
    # Create a Table object with the reference and set the expires property directly
    table_to_update = bigquery.Table(table_ref)
    table_to_update.expires = expiration_timestamp_ms

    # Update the table in BigQuery
    updated_table = client.update_table(table_to_update, ["expires"])
