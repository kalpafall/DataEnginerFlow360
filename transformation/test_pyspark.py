"""
Simple test script to verify PySpark is working
"""
import sys
from pyspark.sql import SparkSession

print("Testing PySpark installation...")

try:
    spark = SparkSession.builder \
        .appName("PySpark_Test") \
        .master("local[*]") \
        .getOrCreate()
    
    print(f"✅ Spark session created successfully!")
    print(f"   Spark version: {spark.version}")
    print(f"   Python version: {sys.version}")
    
    # Create a simple DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    print(f"\n✅ DataFrame created with {df.count()} rows")
    print("\nSample data:")
    df.show()
    
    spark.stop()
    print("\n✅ All tests passed! PySpark is working correctly.")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    sys.exit(1)
