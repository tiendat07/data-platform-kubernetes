import sys
import os
import requests
import gzip
import json
from pyspark.sql.functions import col, to_timestamp, to_date, hour
from utils import get_spark

TARGET_TABLE = "lakehouse.ingestion.github_events"
BATCH_SIZE = 10000

def github_stream_generator(url):
    print(f">>> Opening stream from {url}...")
    try:
        with requests.get(url, stream=True) as r:
            if r.status_code == 404:
                print(f">>> Warning: File not found {url}. Skipping.")
                return
            r.raise_for_status()
            
            with gzip.GzipFile(fileobj=r.raw) as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        yield {
                            "id": str(record.get("id")),
                            "type": record.get("type"),
                            "actor_login": record.get("actor", {}).get("login"),
                            "repo_name": record.get("repo", {}).get("name"),
                            "created_at": record.get("created_at"),
                            "payload": json.dumps(record.get("payload")),
                            # Ensure org_login is None if missing, not Key Error
                            "org_login": record.get("org", {}).get("login") if record.get("org") else None
                        }
                    except Exception as e:
                        continue
    except Exception as e:
        print(f"Stream error: {e}")
        raise e

def flush_batch(spark, data, batch_id):
    if not data:
        return

    print(f"    -> Writing Batch {batch_id} ({len(data)} records)...")
    
    # Create DataFrame
    df_raw = spark.createDataFrame(data)
    
    # Select & Transform
    df_final = df_raw.select(
        col("id"),
        col("type"),
        col("actor_login"),
        col("repo_name"),
        col("created_at"),
        col("payload"),
        col("org_login") # Added org_login here
    )
    
    # Calculate Partition Columns
    df_final = df_final \
        .withColumn("ts", to_timestamp(col("created_at"))) \
        .withColumn("event_date", to_date(col("ts"))) \
        .withColumn("event_hour", hour(col("ts"))) \
        .drop("ts") 

    # APPEND (Safe because we cleaned the partition at step D)
    df_final.writeTo(TARGET_TABLE) \
        .option("mergeSchema", "true") \
        .option("check-ordering", "false") \
        .append()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: github_events.py <date> <hour>")
        sys.exit(1)

    target_date = sys.argv[1]
    target_hour = int(sys.argv[2])

    # Format URL: http://data.gharchive.org/2015-01-01-15.json.gz
    file_name = f"{target_date}-{target_hour}.json.gz"
    url = f"http://data.gharchive.org/{file_name}"
    
    print(f">>> Processing Date: {target_date}, Hour: {target_hour}")
    
    spark, logger = get_spark(job_name=f"GH-Archive-{file_name}")
        
    # --- C. CREATE TABLE (IF NOT EXISTS) ---
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            id STRING,
            type STRING,
            actor_login STRING,
            repo_name STRING,
            created_at STRING,
            payload STRING,
            org_login STRING,
            event_date DATE,
            event_hour INT
        )
        USING iceberg
        PARTITIONED BY (event_date, event_hour)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.spark.accept-any-schema' = 'true'
        )
    """)
    
    # Ensure schema matches (Evolution)
    try:
        existing_cols = [field.name for field in spark.table(TARGET_TABLE).schema]
        if "org_login" not in existing_cols:
            spark.sql(f"ALTER TABLE {TARGET_TABLE} ADD COLUMN org_login STRING")
    except Exception as e:
        print(f"Schema evolution warning: {e}")
    
    # --- D. IDEMPOTENCY STEP (DELETE) ---
    # Before we append new data, remove any data that might exist for this specific hour.
    # This handles "Retry" logic perfectly.
    print(f">>> Cleaning existing data for {target_date} Hour {target_hour}...")
    spark.sql(f"""
        DELETE FROM {TARGET_TABLE} 
        WHERE event_date = '{target_date}' 
          AND event_hour = {target_hour}
    """)
    
    # --- E. STREAMING LOOP ---
    batch_buffer = []
    batch_count = 0
    
    print(f">>> Starting Ingestion from {url}")
    
    for record in github_stream_generator(url):
        batch_buffer.append(record)
        
        if len(batch_buffer) >= BATCH_SIZE:
            flush_batch(spark, batch_buffer, batch_count)
            batch_count += 1
            batch_buffer = [] 
            
    if batch_buffer:
        flush_batch(spark, batch_buffer, batch_count)

    print(">>> SUCCESS.")