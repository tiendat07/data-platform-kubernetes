import sys
import os
import requests
from pyspark.sql.functions import col, to_timestamp, to_date, hour, lit
from utils import get_spark

TARGET_TABLE = "lakehouse.ingestion.github_events"

def download_file(url, local_path):
    print(f">>> Downloading {url} to {local_path}...")
    with requests.get(url, stream=True) as r:
        if r.status_code == 404:
            print(f">>> Warning: File not found {url}.")
            return False
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return True

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: github_events.py <date> <hour>")
        sys.exit(1)

    target_date = sys.argv[1]
    target_hour = int(sys.argv[2])

    file_name = f"{target_date}-{target_hour}.json.gz"
    url = f"http://data.gharchive.org/{file_name}"
    local_temp_path = f"/tmp/{file_name}"
    
    print(f">>> Processing Date: {target_date}, Hour: {target_hour}")
    
    # Initialize Spark
    spark, logger = get_spark(job_name=f"GH-Archive-{file_name}")
    
    # 1. DOWNLOAD FIRST (Much faster than streaming to Python list)
    if not download_file(url, local_temp_path):
        sys.exit(0) # Exit gracefully if file doesn't exist

    try:
        # 2. READ WITH SPARK (Utilizes Spark's native JSON parser)
        # This handles the schema inference automatically.
        print(f">>> Reading JSON into Spark DataFrame...")
        df_raw = spark.read.json(local_temp_path)
        
        # 3. TRANSFORM
        # Note: We use col("org.login") to access nested JSON fields safely
        df_final = df_raw.select(
            col("id").cast("string"),
            col("type"),
            col("actor.login").alias("actor_login"),
            col("repo.name").alias("repo_name"),
            col("created_at"),
            col("payload").cast("string"), # Payload as JSON string
            col("org.login").alias("org_login")
        )

        # Add Partition Columns
        df_final = df_final \
            .withColumn("ts", to_timestamp(col("created_at"))) \
            .withColumn("event_date", to_date(col("ts"))) \
            .withColumn("event_hour", hour(col("ts"))) \
            .drop("ts")

        # 4. IDEMPOTENCY (DELETE OLD DATA FOR THIS HOUR)
        print(f">>> Cleaning existing data for {target_date} Hour {target_hour}...")
        spark.sql(f"""
            DELETE FROM {TARGET_TABLE} 
            WHERE event_date = '{target_date}' 
              AND event_hour = {target_hour}
        """)

        # 5. WRITE ONCE (Creates only 1 Snapshot!)
        print(f">>> Writing to Iceberg...")
        df_final.writeTo(TARGET_TABLE) \
            .option("mergeSchema", "true") \
            .option("check-ordering", "false") \
            .append()
            
        print(">>> SUCCESS.")

    finally:
        # 6. CLEANUP LOCAL FILE
        if os.path.exists(local_temp_path):
            os.remove(local_temp_path)