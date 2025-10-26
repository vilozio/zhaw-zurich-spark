from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Configuration
MAX_BATCH_SIZE = 1000           # Maximum number of messages per micro-batch
PROCESSING_TIME = '10 seconds'  # Processing time for micro-batches
CUSTOMERS_OUTPUT_PATH = "/tmp/customers" # Delta table in HDFS

# Create a Spark session
spark = (
    SparkSession.builder
    .appName("customer-streaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define schema for customers topic (from Debezium CDC)
customers_schema = T.StructType([
    T.StructField("payload", T.StructType([
        T.StructField("customer_id", T.IntegerType()),
        T.StructField("name", T.StringType()),
        T.StructField("birthday", T.DateType()),
        T.StructField("membership_level", T.StringType()),
        T.StructField("shipping_address", T.StringType()),
        T.StructField("created_at", T.TimestampType())
    ]))
])


def process_customers_batch(batch_df, batch_id):
    """
    Process a batch containing customers and write them to Delta table in HDFS
    """
    print(f"\n{'='*80}")
    print(f"=== Processing Customers Batch {batch_id} ===")
    print(f"{'='*80}")
    
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty, skipping...")
        return
    
    print(f"Total messages in batch: {batch_df.count()}")
    
    customers_count = batch_df.count()
    
    print(f"  - Customers messages: {customers_count}")
    
    # Process customers first (if any)
    if customers_count > 0:
        print(f"\n--- Processing {customers_count} customer(s) ---")
        # We need to parse the JSON data from the value column
        customers_batch = (
            batch_df
            .select(F.from_json(F.col("value").cast("string"), customers_schema).alias("data"))
            .select(
                F.col("data.payload.customer_id").alias("customer_id"),
                F.col("data.payload.name").alias("name"),
                F.col("data.payload.created_at").alias("customer_created_at"),
                F.col("data.payload.membership_level").alias("membership_level"),
                F.col("data.payload.shipping_address").alias("shipping_address")
            )
        )
        
        customers_batch.show(5, truncate=False)
        
        # Write customers to HDFS
        (
            customers_batch
            .write
            .format("delta")
            .mode("append") # Append mode to write new customers (duplicates are allowed)
            .option("mergeSchema", "true") # Merge schema if new fields are added
            .save(CUSTOMERS_OUTPUT_PATH)
        )
        
        print(f"✓ {customers_count} customer(s) written to {CUSTOMERS_OUTPUT_PATH}")
    
    print(f"✓ Batch {batch_id} processing complete!")


# Read from Kafka customers topic
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "postgres.public.customers")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", MAX_BATCH_SIZE)
    .load()
)

# Processing topics in micro-batches
customers_stream_query = (
    kafka_stream
    .writeStream
    .queryName("customers_stream")
    .foreachBatch(process_customers_batch)
    .outputMode("append")
    .trigger(processingTime=PROCESSING_TIME)
    .option("checkpointLocation", "/spark-checkpoints/customers_stream")
    .start()
)

print("=" * 80)
print("Streaming job started successfully!")
print(f"Customers stream: {customers_stream_query.id}")
print("\nMonitoring streams... Press Ctrl+C to stop")

# Wait for termination
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n\nStopping streams...")
    customers_stream_query.stop()
    print("Streams stopped successfully!")
