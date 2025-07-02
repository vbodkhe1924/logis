import os
from pyspark.sql import SparkSession

# Remove the problematic PYSPARK_SUBMIT_ARGS
if 'PYSPARK_SUBMIT_ARGS' in os.environ:
    del os.environ['PYSPARK_SUBMIT_ARGS']

# Set Java options directly in Spark config
try:
    spark = SparkSession.builder \
        .appName("LogisticsETLTest") \
        .master("local[*]") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=default") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=default") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print("✓ Spark session created successfully!")
    print(f"✓ Spark version: {spark.version}")
    print(f"✓ Spark UI available at: {spark.sparkContext.uiWebUrl}")
    
    # Test with a simple operation
    test_data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(test_data, ["name", "id"])
    df.show()
    print("✓ DataFrame operations working!")
    
    spark.stop()
    print("✓ Spark session stopped successfully")
    
except Exception as e:
    print(f"✗ Spark session failed: {e}")
    print("\nTrying alternative configuration...")
    
    # Alternative: Try without security manager options
    try:
        spark = SparkSession.builder \
            .appName("LogisticsETLTest") \
            .master("local[*]") \
            .getOrCreate()
        
        print("✓ Spark session created with basic config!")
        print(f"✓ Spark version: {spark.version}")
        
        # Test with a simple operation
        test_data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        df = spark.createDataFrame(test_data, ["name", "id"])
        df.show()
        print("✓ DataFrame operations working!")
        
        spark.stop()
        print("✓ Spark session stopped successfully")
        
    except Exception as e2:
        print(f"✗ Alternative configuration also failed: {e2}")
        print("\nDebugging information:")
        print(f"JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not set')}")
        print(f"SPARK_HOME: {os.environ.get('SPARK_HOME', 'Not set')}")
        print(f"Python version: {os.sys.version}")
        
        # Check if Java is available
        try:
            import subprocess
            result = subprocess.run(['java', '-version'], capture_output=True, text=True)
            print(f"Java version output: {result.stderr}")
        except Exception as java_err:
            print(f"Java not found: {java_err}")