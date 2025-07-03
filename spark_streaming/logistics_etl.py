#!/usr/bin/env python3
"""
Logistics ETL Pipeline configured for Docker environment without Hadoop dependencies
"""

import os
import sys
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Suppress warnings
warnings.filterwarnings("ignore")
os.environ['PYTHONWARNINGS'] = 'ignore'

# Set environment variables to bypass Hadoop
os.environ['SPARK_LOCAL_IP'] = '0.0.0.0'
os.environ['SPARK_LOCAL_HOSTNAME'] = 'etl-app'
os.environ['HADOOP_CONF_DIR'] = '/tmp/empty-hadoop-conf'
os.environ['SPARK_HADOOP_CONF_DIR'] = '/tmp/empty-hadoop-conf'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session in local mode without external dependencies"""
    try:
        # First try cluster mode, fallback to local mode
        spark_configs = [
            {
                "name": "Cluster Mode",
                "master": "spark://spark-master:7077",
                "jars": "/app/jars/postgresql-42.7.6.jar,/app/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar"
            },
            {
                "name": "Local Mode",
                "master": "local[*]",
                "jars": None
            }
        ]
        
        for config in spark_configs:
            try:
                logger.info(f"Attempting to create Spark session in {config['name']}...")
                
                builder = SparkSession.builder \
                    .appName("LogisticsETL") \
                    .master(config["master"]) \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .config("spark.executor.memory", "1g") \
                    .config("spark.driver.memory", "1g") \
                    .config("spark.driver.host", "0.0.0.0") \
                    .config("spark.driver.bindAddress", "0.0.0.0") \
                    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
                    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
                    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
                    .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
                    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                    .config("spark.hadoop.fs.defaultFS", "file:///") \
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
                    .config("spark.hadoop.fs.file.impl.disable.cache", "true")
                
                if config["jars"] and os.path.exists("/app/jars/postgresql-42.7.6.jar"):
                    builder = builder.config("spark.jars", config["jars"])
                
                spark = builder.getOrCreate()
                spark.sparkContext.setLogLevel("ERROR")
                logger.info(f"✓ Spark session created successfully in {config['name']}")
                logger.info(f"Spark version: {spark.version}")
                logger.info(f"Spark master: {spark.sparkContext.master}")
                return spark
                
            except Exception as e:
                logger.warning(f"Failed to create Spark session in {config['name']}: {e}")
                continue
        
        logger.error("✗ Failed to create Spark session in any mode")
        return None
        
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {e}")
        return None

def test_connections():
    """Test all service connections with proper error handling"""
    connections = {'redis': False, 'postgres': False}
    
    # Test Redis
    try:
        import redis
        r = redis.Redis(host='redis', port=6379, db=0, socket_connect_timeout=5)
        r.ping()
        logger.info("✓ Redis connection successful")
        connections['redis'] = True
    except ImportError:
        logger.warning("⚠ Redis library not available")
    except Exception as e:
        logger.error(f"✗ Redis connection failed: {e}")
        logger.info("⚠ Continuing without Redis...")
    
    # Test PostgreSQL
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="logistics-postgres",
            port="5432",
            database="logistics_db",
            user="vb",
            password="varad123",
            connect_timeout=5
        )
        conn.close()
        logger.info("✓ Database connection successful")
        connections['postgres'] = True
    except ImportError:
        logger.warning("⚠ PostgreSQL library not available")
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        logger.info("⚠ Database connection failed. ETL will run without database persistence.")
    
    return connections

def test_kafka_connection():
    """Test Kafka connection with fallback"""
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
        
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            api_version=(0, 10, 1),
            request_timeout_ms=5000
        )
        producer.close()
        logger.info("✓ Kafka connection successful")
        return True
    except ImportError:
        logger.warning("⚠ kafka-python not installed")
        return False
    except Exception as e:
        logger.error(f"✗ Kafka connection failed: {e}")
        logger.info("⚠ Starting in mock data mode")
        return False

def create_mock_data_stream(spark, topic_name):
    """Create mock streaming data when Kafka is not available"""
    try:
        logger.info(f"📊 Creating mock data stream for {topic_name}...")
        
        if topic_name == "driver-locations":
            return spark.readStream.format("rate").option("rowsPerSecond", 1).load() \
                .withColumn("driver_id", concat(lit("driver_"), (col("value") % 5).cast("string"))) \
                .withColumn("latitude", 40.7128 + (rand() - 0.5) * 0.1) \
                .withColumn("longitude", -74.0060 + (rand() - 0.5) * 0.1) \
                .withColumn("speed", 20 + rand() * 60) \
                .withColumn("heading", rand() * 360) \
                .select("driver_id", "latitude", "longitude", "timestamp", "speed", "heading")
                
        elif topic_name == "delivery-status":
            return spark.readStream.format("rate").option("rowsPerSecond", 1).load() \
                .withColumn("delivery_id", concat(lit("del_"), col("value").cast("string"))) \
                .withColumn("status", 
                    when(col("value") % 3 == 0, "delivered")
                    .when(col("value") % 3 == 1, "in_transit")
                    .otherwise("picked_up")) \
                .withColumn("location", lit("NYC")) \
                .withColumn("driver_id", concat(lit("driver_"), (col("value") % 5).cast("string"))) \
                .withColumn("customer_id", concat(lit("cust_"), col("value").cast("string"))) \
                .select("delivery_id", "status", "timestamp", "location", "driver_id", "customer_id")
        
        return None
    except Exception as e:
        logger.error(f"✗ Failed to create mock data stream: {e}")
        return None

def process_driver_locations(spark, use_kafka=True):
    """Process driver location data from Kafka or mock data"""
    try:
        logger.info("📍 Starting driver location processing...")
        
        if use_kafka:
            try:
                location_schema = StructType([
                    StructField("driver_id", StringType(), True),
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                    StructField("timestamp", TimestampType(), True),
                    StructField("speed", DoubleType(), True),
                    StructField("heading", DoubleType(), True)
                ])
                
                df = spark \
                    .readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:29092") \
                    .option("subscribe", "driver-locations") \
                    .option("startingOffsets", "latest") \
                    .option("failOnDataLoss", "false") \
                    .load()
                
                parsed_df = df.select(
                    from_json(col("value").cast("string"), location_schema).alias("data")
                ).select("data.*")
            except Exception as e:
                logger.warning(f"Kafka streaming failed, using mock data: {e}")
                parsed_df = create_mock_data_stream(spark, "driver-locations")
        else:
            parsed_df = create_mock_data_stream(spark, "driver-locations")
        
        if parsed_df is None:
            return None
            
        processed_df = parsed_df.withColumn("processed_at", current_timestamp())
        
        query = processed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        logger.info("✓ Driver location processing started")
        return query
    except Exception as e:
        logger.error(f"✗ Failed to start driver location processing: {e}")
        return None

def process_delivery_status(spark, use_kafka=True):
    """Process delivery status updates from Kafka or mock data"""
    try:
        logger.info("📦 Starting delivery status processing...")
        
        if use_kafka:
            try:
                delivery_schema = StructType([
                    StructField("delivery_id", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("timestamp", TimestampType(), True),
                    StructField("location", StringType(), True),
                    StructField("driver_id", StringType(), True),
                    StructField("customer_id", StringType(), True)
                ])
                
                df = spark \
                    .readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:29092") \
                    .option("subscribe", "delivery-status") \
                    .option("startingOffsets", "latest") \
                    .option("failOnDataLoss", "false") \
                    .load()
                
                parsed_df = df.select(
                    from_json(col("value").cast("string"), delivery_schema).alias("data")
                ).select("data.*")  # Fixed the typo here
            except Exception as e:
                logger.warning(f"Kafka streaming failed, using mock data: {e}")
                parsed_df = create_mock_data_stream(spark, "delivery-status")
        else:
            parsed_df = create_mock_data_stream(spark, "delivery-status")
        
        if parsed_df is None:
            return None
            
        processed_df = parsed_df.withColumn("processed_at", current_timestamp())
        
        query = processed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        logger.info("✓ Delivery status processing started")
        return query
    except Exception as e:
        logger.error(f"✗ Failed to start delivery status processing: {e}")
        return None

def calculate_kpis(spark, use_kafka=True):
    """Calculate real-time KPIs"""
    try:
        logger.info("📊 Starting KPI calculations...")
        
        if use_kafka:
            try:
                delivery_schema = StructType([
                    StructField("delivery_id", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("timestamp", TimestampType(), True),
                    StructField("driver_id", StringType(), True)
                ])
                
                df = spark \
                    .readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:29092") \
                    .option("subscribe", "delivery-status") \
                    .option("startingOffsets", "latest") \
                    .option("failOnDataLoss", "false") \
                    .load()
                
                parsed_df = df.select(
                    from_json(col("value").cast("string"), delivery_schema).alias("data")
                ).select("data.*")
            except Exception as e:
                logger.warning(f"Kafka streaming failed, using mock data: {e}")
                parsed_df = spark.readStream.format("rate").option("rowsPerSecond", 2).load() \
                    .withColumn("delivery_id", concat(lit("del_"), col("value").cast("string"))) \
                    .withColumn("status", 
                        when(col("value") % 3 == 0, "delivered")
                        .when(col("value") % 3 == 1, "in_transit")
                        .otherwise("picked_up")) \
                    .withColumn("driver_id", concat(lit("driver_"), (col("value") % 5).cast("string"))) \
                    .select("delivery_id", "status", "timestamp", "driver_id")
        else:
            parsed_df = spark.readStream.format("rate").option("rowsPerSecond", 2).load() \
                .withColumn("delivery_id", concat(lit("del_"), col("value").cast("string"))) \
                .withColumn("status", 
                    when(col("value") % 3 == 0, "delivered")
                    .when(col("value") % 3 == 1, "in_transit")
                    .otherwise("picked_up")) \
                .withColumn("driver_id", concat(lit("driver_"), (col("value") % 5).cast("string"))) \
                .select("delivery_id", "status", "timestamp", "driver_id")
        
        kpis = parsed_df \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "1 minute"),
                col("status")
            ) \
            .count() \
            .withColumnRenamed("count", "delivery_count") \
            .withColumn("kpi_calculated_at", current_timestamp())
        
        query = kpis.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 20) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        logger.info("✓ KPI calculations started")
        return query
    except Exception as e:
        logger.error(f"✗ Failed to start KPI calculations: {e}")
        return None

def main():
    """Main ETL pipeline execution"""
    logger.info("🚀 Starting Logistics ETL Pipeline...")
    
    # Test connections (non-blocking)
    connections = test_connections()
    kafka_available = test_kafka_connection()
    
    # Create Spark session with fallback
    spark = create_spark_session()
    if not spark:
        logger.error("❌ Cannot proceed without Spark session")
        return
    
    # Start streaming queries
    queries = []
    
    # Process driver locations
    driver_query = process_driver_locations(spark, use_kafka=kafka_available)
    if driver_query:
        queries.append(driver_query)
    
    # Process delivery status
    delivery_query = process_delivery_status(spark, use_kafka=kafka_available)
    if delivery_query:
        queries.append(delivery_query)
    
    # Calculate KPIs
    kpi_query = calculate_kpis(spark, use_kafka=kafka_available)
    if kpi_query:
        queries.append(kpi_query)
    
    logger.info(f"✅ Started {len(queries)} streaming queries")
    
    if not kafka_available:
        logger.info("🔄 Running in MOCK DATA MODE - Kafka not available")
        logger.info("   To use real Kafka streams, ensure Kafka is running on kafka:29092")
    
    if queries:
        logger.info("🎯 ETL pipeline is running. Press Ctrl+C to stop...")
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("🛑 Stopping ETL pipeline...")
            for query in queries:
                query.stop()
        finally:
            spark.stop()
            logger.info("✅ ETL pipeline stopped successfully")
    else:
        logger.error("❌ No streaming queries started. Check your configuration.")
        spark.stop()

if __name__ == "__main__":
    main()