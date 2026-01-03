import os
from utils import get_spark

spark, logger = get_spark(job_name="main_spark")
# Create Iceberg table "nyc.taxis_large" from RDD
# df.write.mode("overwrite").saveAsTable("raw.taxis_spark")
# df.writeTo("raw.taxis_spark").tableProperty(
#     "write.format.default", "parquet"
# ).partitionedBy("y", "m").createOrReplace()
# # Query table row count
# count_df = spark.sql("SELECT COUNT(*) AS cnt FROM raw.taxis_spark")
# total_rows_count = count_df.first().cnt
df = spark.sql("""
  SELECT created_at
  FROM lakehouse.ingestion.github_events
  ORDER BY created_at ASC
  limit 10
""")
df.show(truncate=False)
# total_rows_count = df.head(10)
# logger.info(f"Total Rows for GitHub Events: {total_rows_count}")