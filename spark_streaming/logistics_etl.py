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
import redis
import psycopg2
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
    """Create Spark session in local mode without Hadoop dependencies"""
    try:
        spark = SparkSession.builder \
            .appName("LogisticsETL") \
            .master("spark://spark-master:7077") \
            .config("spark.jars", "/app/jars/postgresql-42.7.6.jar,/app/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.host", "etl-app") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
            .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
            .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("✓ Spark session created successfully")
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Spark master: {spark.sparkContext.master}")
        return spark
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {e}")
        return None

def test_connections():
    """Test all service connections"""
    connections = {'redis': False, 'postgres': False}
    
    # Test Redis
    try:
        r = redis.Redis(host='redis', port=6379, db=0, socket_connect_timeout=5)
        r.ping()
        logger.info("✓ Redis connection successful")
        connections['redis'] = True
    except Exception as e:
        logger.error(f"✗ Redis connection failed: {e}")
        logger.info("⚠ Continuing without Redis...")
    
    # Test PostgreSQL
    try:
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
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        logger.info("⚠ Database connection failed. ETL will run without database persistence.")
    
    return connections

def test_kafka_connection():
    """Test Kafka connection"""
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
    except NoBrokersAvailable:
        logger.error("✗ Kafka broker not available at kafka:29092")
        logger.info("⚠ Starting in mock data mode")
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
            schema = StructType([
                StructField("driver_id", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("speed", DoubleType(), True),
                StructField("heading", DoubleType(), True)
            ])
            mock_data = [
                ("driver_001", 40.7128, -74.0060, "2024-06-25 10:00:00", 45.5, 180.0),
                ("driver_002", 40.7589, -73.9851, "2024-06-25 10:01:00", 30.2, 90.0),
                ("driver_003", 40.7505, -73.9934, "2024-06-25 10:02:00", 25.8, 270.0)
            ]
        elif topic_name == "delivery-status":
            schema = StructType([
                StructField("delivery_id", StringType(), True),
                StructField("status", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("location", StringType(), True),
                StructField("driver_id", StringType(), True),
                StructField("customer_id", StringType(), True)
            ])
            mock_data = [
                ("del_001", "delivered", "2024-06-25 10:00:00", "NYC", "driver_001", "cust_001"),
                ("del_002", "in_transit", "2024-06-25 10:01:00", "NYC", "driver_002", "cust_002"),
                ("del_003", "picked_up", "2024-06-25 10:02:00", "NYC", "driver_003", "cust_003")
            ]
        
        df = spark.createDataFrame(mock_data, schema)
        df = df.withColumn("timestamp", current_timestamp())
        return df
    except Exception as e:
        logger.error(f"✗ Failed to create mock data stream: {e}")
        return None

def process_driver_locations(spark, use_kafka=True):
    """Process driver location data from Kafka or mock data"""
    try:
        logger.info("📍 Starting driver location processing...")
        
        location_schema = StructType([
            StructField("driver_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("speed", DoubleType(), True),
            StructField("heading", DoubleType(), True)
        ])
        
        if use_kafka:
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
        else:
            mock_df = create_mock_data_stream(spark, "driver-locations")
            if mock_df is None:
                return None
            parsed_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load() \
                .withColumn("driver_id", lit("mock_driver")) \
                .withColumn("latitude", lit(40.7128)) \
                .withColumn("longitude", lit(-74.0060)) \
                .withColumn("speed", lit(45.5)) \
                .withColumn("heading", lit(180.0)) \
                .select("driver_id", "latitude", "longitude", "timestamp", "speed", "heading")
        
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
        
        delivery_schema = StructType([
            StructField("delivery_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("customer_id", StringType(), True)
        ])
        
        if use_kafka:
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
            ).select(" Writer: data.*")
        else:
            parsed_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load() \
                .withColumn("delivery_id", concat(lit("del_"), col("value").cast("string"))) \
                .withColumn("status", lit("delivered")) \
                .withColumn("location", lit("NYC")) \
                .withColumn("driver_id", lit("mock_driver")) \
                .withColumn("customer_id", concat(lit("cust_"), col("value").cast("string"))) \
                .select("delivery_id", "status", "timestamp", "location", "driver_id", "customer_id")
        
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
            df = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "delivery-status") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            delivery_schema = StructType([
                StructField("delivery_id", StringType(), True),
                StructField("status", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("driver_id", StringType(), True)
            ])
            
            parsed_df = df.select(
                from_json(col("value").cast("string"), delivery_schema).alias("data")
            ).select("data.*")
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
    
    connections = test_connections()
    kafka_available = test_kafka_connection()
    
    spark = create_spark_session()
    if not spark:
        logger.error("❌ Cannot proceed without Spark session")
        return
    
    queries = []
    
    driver_query = process_driver_locations(spark, use_kafka=kafka_available)
    if driver_query:
        queries.append(driver_query)
    
    delivery_query = process_delivery_status(spark, use_kafka=kafka_available)
    if delivery_query:
        queries.append(delivery_query)
    
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