from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import redis
import psycopg2
from psycopg2.extras import RealDictCursor

class LogisticsETL:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("LogisticsETL") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        
        # Database connection
        self.db_config = {
            'host': 'localhost',
            'database': 'logistics_db',
            'user': 'admin',
            'password': 'password'
        }
        
        self.setup_database()
    
    def setup_database(self):
        """Initialize database tables"""
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                # Driver locations table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS driver_locations (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP,
                        driver_id VARCHAR(50),
                        latitude DOUBLE PRECISION,
                        longitude DOUBLE PRECISION,
                        speed DOUBLE PRECISION,
                        heading DOUBLE PRECISION,
                        status VARCHAR(20)
                    )
                """)
                
                # Delivery analytics table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS delivery_analytics (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP,
                        order_id VARCHAR(50),
                        driver_id VARCHAR(50),
                        status VARCHAR(30),
                        delay_minutes INTEGER,
                        delay_reason VARCHAR(100)
                    )
                """)
                
                # Real-time KPIs table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS realtime_kpis (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP,
                        metric_name VARCHAR(50),
                        metric_value DOUBLE PRECISION,
                        area_id VARCHAR(20)
                    )
                """)
                
                conn.commit()
    
    def process_driver_locations(self):
        """Process driver location stream"""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "driver_locations") \
            .load()
        
        # Parse JSON data
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("heading", DoubleType(), True),
            StructField("status", StringType(), True)
        ])
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Add processing timestamp
        processed_df = parsed_df.withColumn("processed_at", current_timestamp())
        
        # Write to PostgreSQL
        query = processed_df.writeStream \
            .outputMode("append") \
            .foreachBatch(self.write_to_postgres) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        return query
    
    def process_delivery_status(self):
        """Process delivery status with anomaly detection"""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "delivery_status") \
            .load()
        
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("delay_reason", StringType(), True),
            StructField("delay_minutes", IntegerType(), True)
        ])
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Anomaly detection for delays
        anomaly_df = parsed_df.filter(
            (col("delay_minutes") > 30) | 
            (col("status") == "cancelled")
        ).withColumn("anomaly_type", 
            when(col("delay_minutes") > 30, "high_delay")
            .when(col("status") == "cancelled", "cancellation")
            .otherwise("unknown")
        )
        
        # Write anomalies to Redis for real-time alerts
        anomaly_query = anomaly_df.writeStream \
            .outputMode("append") \
            .foreachBatch(self.write_anomalies_to_redis) \
            .trigger(processingTime='5 seconds') \
            .start()
        
        # Write all delivery data to PostgreSQL
        delivery_query = parsed_df.writeStream \
            .outputMode("append") \
            .foreachBatch(self.write_delivery_analytics_to_postgres) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        return [anomaly_query, delivery_query]
    
    def calculate_realtime_kpis(self):
        """Calculate real-time KPIs"""
        # Driver locations KPIs
        driver_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "driver_locations") \
            .load()
        
        # Delivery status KPIs
        delivery_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "delivery_status") \
            .load()
        
        # Parse and calculate metrics
        driver_schema = StructType([
            StructField("driver_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("speed", DoubleType(), True)
        ])
        
        parsed_drivers = driver_df.select(
            from_json(col("value").cast("string"), driver_schema).alias("data")
        ).select("data.*")
        
        # Calculate active drivers count
        active_drivers = parsed_drivers \
            .filter(col("status") != "idle") \
            .groupBy(window(current_timestamp(), "1 minute")) \
            .agg(countDistinct("driver_id").alias("active_drivers"))
        
        # Write KPIs
        kpi_query = active_drivers.writeStream \
            .outputMode("update") \
            .foreachBatch(self.write_kpis_to_postgres) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        return kpi_query
    
    def write_to_postgres(self, df, epoch_id):
        """Write DataFrame to PostgreSQL"""
        if df.count() > 0:
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://localhost:5432/{self.db_config['database']}") \
                .option("dbtable", "driver_locations") \
                .option("user", self.db_config['user']) \
                .option("password", self.db_config['password']) \
                .mode("append") \
                .save()
    
    def write_delivery_analytics_to_postgres(self, df, epoch_id):
        """Write delivery analytics to PostgreSQL"""
        if df.count() > 0:
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://localhost:5432/{self.db_config['database']}") \
                .option("dbtable", "delivery_analytics") \
                .option("user", self.db_config['user']) \
                .option("password", self.db_config['password']) \
                .mode("append") \
                .save()
    
    def write_anomalies_to_redis(self, df, epoch_id):
        """Write anomalies to Redis for real-time alerts"""
        anomalies = df.collect()
        for row in anomalies:
            alert_data = {
                'timestamp': row['timestamp'],
                'order_id': row['order_id'],
                'driver_id': row['driver_id'],
                'anomaly_type': row['anomaly_type'],
                'delay_minutes': row['delay_minutes'],
                'delay_reason': row['delay_reason']
            }
            
            # Store in Redis with TTL of 1 hour
            self.redis_client.setex(
                f"alert:{row['order_id']}", 
                3600, 
                json.dumps(alert_data, default=str)
            )
    
    def write_kpis_to_postgres(self, df, epoch_id):
        """Write KPIs to PostgreSQL"""
        kpis = df.collect()
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                for row in kpis:
                    cur.execute("""
                        INSERT INTO realtime_kpis (timestamp, metric_name, metric_value)
                        VALUES (%s, %s, %s)
                    """, (row['window']['start'], 'active_drivers', row['active_drivers']))
                conn.commit()
    
    def start_streaming(self):
        """Start all streaming queries"""
        queries = []
        
        # Start driver location processing
        queries.append(self.process_driver_locations())
        
        # Start delivery status processing
        delivery_queries = self.process_delivery_status()
        queries.extend(delivery_queries)
        
        # Start KPI calculations
        queries.append(self.calculate_realtime_kpis())
        
        return queries

if __name__ == "__main__":
    etl = LogisticsETL()
    queries = etl.start_streaming()
    
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming...")
        for query in queries:
            query.stop()