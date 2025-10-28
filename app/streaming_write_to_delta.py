from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Configuration
MAX_BATCH_SIZE = 1000           # Maximum number of messages per micro-batch
PROCESSING_TIME = '10 seconds'  # Processing time for micro-batches
CUSTOMERS_OUTPUT_PATH = "/tmp/customers" # Delta table in HDFS
ORDERS_OUTPUT_PATH = "/tmp/orders"       # Delta table for orders in HDFS

# Create a Spark session
spark = (
    SparkSession.builder
    .appName("customer-and-orders-streaming")
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

# TODO 1: Define schema for orders topic (from Debezium CDC)
# Hint: Look at the postgres/initdb/init-all.sql to see the orders table structure
# The schema should include: order_id, customer_id, product, cost, description, created_at, credit_card_number
orders_schema = T.StructType([
    T.StructField("payload", T.StructType([
        # TODO: Add the fields here
        # Example: T.StructField("order_id", T.IntegerType()),
        # ...
    ]))
])


def process_batch(batch_df, batch_id):
    """
    Process a batch containing both customers and orders, and write them to separate Delta tables
    """
    print(f"\n{'='*80}")
    print(f"=== Processing Batch {batch_id} ===")
    print(f"{'='*80}")
    
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty, skipping...")
        return
    
    print(f"Total messages in batch: {batch_df.count()}")
    
    # Split the batch by topic
    customers_df = batch_df.filter(F.col("topic") == "postgres.public.customers")
    orders_df = batch_df.filter(F.col("topic") == "postgres.public.orders")
    
    customers_count = customers_df.count()
    orders_count = orders_df.count()
    
    print(f"  - Customers messages: {customers_count}")
    print(f"  - Orders messages: {orders_count}")
    
    # ========================================
    # PROCESS CUSTOMERS
    # ========================================
    if customers_count > 0:
        print(f"\n--- Processing {customers_count} customer(s) ---")
        customers_batch = (
            customers_df
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
        
        # Write customers to Delta Lake
        (
            customers_batch
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(CUSTOMERS_OUTPUT_PATH)
        )
        
        print(f"✓ {customers_count} customer(s) written to {CUSTOMERS_OUTPUT_PATH}")
    
    # ========================================
    # TODO 2: PROCESS ORDERS
    # ========================================
    # TODO: Complete the orders processing section
    # Hint: Follow the same pattern as customers processing above
    
    if orders_count > 0:
        print(f"\n--- Processing {orders_count} order(s) ---")
        
        # TODO: Parse the orders data from Kafka
        # Step 1: Use F.from_json to parse the value column with orders_schema
        # Step 2: Select the fields from the payload
        orders_batch = (
            orders_df
            # TODO: Add your code here
            # .select(...)
            # .select(...)
        )
        
        # TODO: Show sample data
        # orders_batch.show(5, truncate=False)
        
        # TODO: Write orders to Delta Lake at ORDERS_OUTPUT_PATH
        # Use the same pattern as customers: format("delta"), mode("append"), mergeSchema
        # (
        #     orders_batch
        #     ...
        # )
        
        print(f"✓ {orders_count} order(s) written to {ORDERS_OUTPUT_PATH}")
    
    print(f"✓ Batch {batch_id} processing complete!")


# TODO 3: Subscribe to both Kafka topics
# Hint: Use comma-separated topic names: "postgres.public.customers,postgres.public.orders"
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "postgres.public.customers")  # TODO: Add orders topic here
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", MAX_BATCH_SIZE)
    .load()
)

# Processing both topics in micro-batches
stream_query = (
    kafka_stream
    .writeStream
    .queryName("customers_and_orders_stream")
    .foreachBatch(process_batch)
    .outputMode("append")
    .trigger(processingTime=PROCESSING_TIME)
    .option("checkpointLocation", "/spark-checkpoints/customers_orders_stream")
    .start()
)

print("=" * 80)
print("Streaming job started successfully!")
print(f"Stream ID: {stream_query.id}")
print("\nMonitoring stream... Press Ctrl+C to stop")

# Wait for termination
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n\nStopping stream...")
    stream_query.stop()
    print("Streams stopped successfully!")
