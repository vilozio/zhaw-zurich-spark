"""
Customer Retention Streaming Application

1. Streams customers and orders from Kafka (Debezium CDC)
2. Writes them to Delta Lake tables
3. Identifies at-risk customers using complex SQL aggregation
4. Writes results to retention analysis table
"""

from datetime import datetime, timedelta
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ========================================
# CONFIGURATION
# ========================================
MAX_BATCH_SIZE = 1000
PROCESSING_TIME = '10 seconds'
DATA_WINDOW_HOURS = 24  # Look at customers from last 24 hours

# Delta table paths
CUSTOMERS_TABLE_PATH = "/user/hive/warehouse/customers_retention"
ORDERS_TABLE_PATH = "/user/hive/warehouse/orders_retention"
RISK_ANALYSIS_TABLE_PATH = "/user/hive/warehouse/customer_risk_analysis"
CHECKPOINT_PATH = "/spark-checkpoints/retention_stream"

# ========================================
# SPARK SESSION
# ========================================
spark = (
    SparkSession.builder
    .appName("customer-retention-streaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session Started")

# ========================================
# SCHEMAS
# ========================================
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

orders_schema = T.StructType([
    T.StructField("payload", T.StructType([
        T.StructField("order_id", T.IntegerType()),
        T.StructField("customer_id", T.IntegerType()),
        T.StructField("product", T.StringType()),
        T.StructField("cost", T.FloatType()),
        T.StructField("description", T.StringType()),
        T.StructField("created_at", T.TimestampType()),
        T.StructField("credit_card_number", T.StringType())
    ]))
])

# ========================================
# AGGREGATION SQL (Spark SQL version)
# ========================================
RETENTION_SQL = """
WITH first_time_customers AS (
    SELECT 
        c.customer_id,
        c.name,
        c.created_at as registration_time,
        c.membership_level
    FROM recent_customers c
),
customer_orders AS (
    SELECT
        o.customer_id,
        o.order_id,
        o.product,
        o.cost,
        o.created_at as order_time,
        CASE 
            WHEN o.description LIKE '%cancelled%' OR o.description LIKE '%canceled%' THEN 1 
            ELSE 0 
        END as is_cancelled
    FROM recent_orders o
    INNER JOIN first_time_customers ftc ON o.customer_id = ftc.customer_id
),
order_aggregates AS (
    SELECT
        co.customer_id,
        COUNT(co.order_id) as total_orders,
        COUNT(DISTINCT co.product) as unique_products,
        AVG(co.cost) as avg_order_value,
        SUM(co.cost) as total_spent,
        SUM(co.is_cancelled) as cancelled_orders,
        MIN(co.order_time) as first_order_time,
        MAX(co.order_time) as last_order_time,
        (unix_timestamp(MAX(co.order_time)) - unix_timestamp(MIN(co.order_time))) / 3600.0 as session_duration_hours
    FROM customer_orders co
    GROUP BY co.customer_id
),
evaluate_churn_risk AS (
    SELECT
        oa.*,
        ftc.name,
        ftc.registration_time,
        ftc.membership_level,
        ROUND((cancelled_orders / NULLIF(total_orders, 0)) * 100, 0) as cancellation_rate,
        (unix_timestamp(current_timestamp()) - unix_timestamp(last_order_time)) / 60.0 as minutes_since_last_order,
        CASE
            WHEN total_orders >= 2
                AND session_duration_hours < 2
                AND (cancelled_orders / NULLIF(total_orders, 0)) > 0.3
                AND unique_products < 3
                AND (unix_timestamp(current_timestamp()) - unix_timestamp(last_order_time)) < 60
            THEN 1
            ELSE 0
        END as at_risk_customer,
        -- A/B test group assignment
        CASE 
            WHEN customer_id % 2 = 0 THEN 'control'
            ELSE 'treatment'
        END as test_group
    FROM order_aggregates oa
    JOIN first_time_customers ftc ON oa.customer_id = ftc.customer_id
)
SELECT
    customer_id,
    name,
    registration_time,
    membership_level,
    total_orders,
    unique_products,
    avg_order_value,
    total_spent,
    cancelled_orders,
    cancellation_rate,
    first_order_time,
    last_order_time,
    session_duration_hours,
    minutes_since_last_order,
    at_risk_customer,
    test_group,
    -- Only send to retention campaign if at risk AND in treatment group
    CASE 
        WHEN at_risk_customer = 1 AND test_group = 'treatment' THEN TRUE
        ELSE FALSE
    END as trigger_retention_campaign
FROM evaluate_churn_risk
"""

# ========================================
# BATCH PROCESSING FUNCTION
# ========================================
def process_unified_batch(batch_df, batch_id):
    """Process unified batch containing both customers and orders"""
    
    print(f"\n{'='*80}")
    print(f"🔥 PROCESSING BATCH {batch_id}")
    print(f"{'='*80}")
    
    if batch_df.isEmpty():
        print(f"⚠️  Empty batch {batch_id}, skipping...")
        return
    
    # Calculate time window for filtering
    now = datetime.now()
    window_start = now - timedelta(hours=DATA_WINDOW_HOURS)
    
    print(f"🕐 Time window: {window_start} to {now} ({DATA_WINDOW_HOURS}h)")
    
    # ========================================
    # SPLIT BY TOPIC
    # ========================================
    customers_raw = batch_df.filter(F.col("topic") == "postgres.public.customers")
    orders_raw = batch_df.filter(F.col("topic") == "postgres.public.orders")
    
    customers_count = customers_raw.count()
    orders_count = orders_raw.count()
    
    print(f"📊 Batch contains: {customers_count} customers, {orders_count} orders")
    
    # ========================================
    # PROCESS & WRITE CUSTOMERS
    # ========================================
        
    customers_parsed = (
        customers_raw
        .withColumn("parsed_data", F.from_json(F.col("value").cast("string"), customers_schema))
        .filter(F.col("parsed_data").isNotNull())
        .filter(F.col("parsed_data.payload.op").isin("c", "r"))  # Create or Read operations
        .select(
            F.col("parsed_data.payload.customer_id").alias("customer_id"),
            F.col("parsed_data.payload.name").alias("name"),
            F.col("parsed_data.payload.birthday").alias("birthday"),
            F.col("parsed_data.payload.membership_level").alias("membership_level"),
            F.col("parsed_data.payload.shipping_address").alias("shipping_address"),
            F.col("parsed_data.payload.created_at").alias("created_at"),
            F.current_timestamp().alias("processed_at"),
        )
    )
    
    # Write to Delta
    (
        customers_parsed
        .write
        .format("delta")
        .mode("append")
        .partitionBy("created_at_date", "created_at_hour")
        .option("mergeSchema", "true")
        .save(CUSTOMERS_TABLE_PATH)
    )
    print("✅ Customers written to Delta")
    
    # ========================================
    # PROCESS & WRITE ORDERS
    # ========================================

    orders_parsed = (
        orders_raw
        .withColumn("parsed_data", F.from_json(F.col("value").cast("string"), orders_schema))
        .filter(F.col("parsed_data").isNotNull())
        .filter(F.col("parsed_data.payload.op").isin("c", "r"))  # Create or Read operations
        .select(
            F.col("parsed_data.payload.order_id").alias("order_id"),
            F.col("parsed_data.payload.customer_id").alias("customer_id"),
            F.col("parsed_data.payload.product").alias("product"),
            F.col("parsed_data.payload.cost").alias("cost"),
            F.col("parsed_data.payload.description").alias("description"),
            F.col("parsed_data.payload.created_at").alias("created_at"),
            F.current_timestamp().alias("processed_at"),
        )
    )
    
    # Write to Delta
    (   
        orders_parsed
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(ORDERS_TABLE_PATH)
    )
    print("✅ Orders written to Delta")

    # ========================================
    # RUN RISK ANALYSIS AGGREGATION
    # ========================================
    print(f"\n--- Running Risk Analysis Aggregation ---")
    
    # Read recent data from Delta tables
    recent_customers = (
        spark.read.format("delta")
        .load(CUSTOMERS_TABLE_PATH)
        .filter(F.col("created_at") >= F.lit(window_start))
    )
    
    recent_orders = (
        spark.read.format("delta")
        .load(ORDERS_TABLE_PATH)
        .filter(F.col("created_at") >= F.lit(window_start))
    )
    
    print(f"📊 Analyzing {recent_customers.count()} recent customers with {recent_orders.count()} orders")
    
    # Create temp views for SQL
    recent_customers.createOrReplaceTempView("recent_customers")
    recent_orders.createOrReplaceTempView("recent_orders")
    
    # Run aggregation SQL
    risk_analysis = spark.sql(RETENTION_SQL)
    risk_analysis = risk_analysis.withColumn("analysis_timestamp", F.current_timestamp())
    risk_analysis = risk_analysis.withColumn("processed_batch_id", F.lit(batch_id))
    
    # Show sample results
    print("\n📋 Sample Risk Analysis Results:")
    risk_analysis.select(
        "customer_id", "name", "total_orders", "unique_products", 
        "cancellation_rate", "at_risk_customer", "trigger_retention_campaign"
    ).show(10, truncate=False)
    
    # Count at-risk customers
    at_risk_count = risk_analysis.filter(F.col("at_risk_customer") == 1).count()
    trigger_count = risk_analysis.filter(F.col("trigger_retention_campaign") == True).count()
    print(f"🚨 Found {at_risk_count} at-risk customers ({trigger_count} will receive retention campaign)")
    
    # Write results to Delta
    (
        risk_analysis
        .write
        .format("delta")
        .mode("append")
        .partitionBy("at_risk_customer")
        .option("mergeSchema", "true")
        .save(RISK_ANALYSIS_TABLE_PATH)
    )
    print("✅ Risk analysis written to Delta")
    
    # ========================================
    # BATCH COMPLETE
    # ========================================


# ========================================
# MAIN STREAMING APPLICATION
# ========================================
if __name__ == "__main__":
    print("🚀 Starting Customer Retention Streaming Application")
    
    # Read from Kafka (both customers and orders topics)
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "postgres.public.customers,postgres.public.orders")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_BATCH_SIZE)
        .load()
    )
    
    # Start streaming with foreachBatch
    stream_query = (
        kafka_stream
        .writeStream
        .queryName("retention_stream")
        .foreachBatch(process_unified_batch)
        .outputMode("append")
        .trigger(processingTime=PROCESSING_TIME)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )
    
    print("\n" + "="*80)
    print("✅ Streaming job started successfully!")
    print(f"📡 Stream ID: {stream_query.id}")
    print("="*80)
    print("\n👀 Monitoring stream... Press Ctrl+C to stop\n")
    
    # Wait for termination
    try:
        stream_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopping stream...")
        stream_query.stop()
        print("✅ Stream stopped successfully!")