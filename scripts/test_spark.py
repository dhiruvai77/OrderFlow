
import os
import sys
from pyspark.sql import SparkSession

# Use the Python interpreter from our virtual environment
os.environ["PYSPARK_PYTHON"] = sys.executable

print("=" * 50)
print("ORDERFLOW - PYSPARK ENVIRONMENT TEST")
print("=" * 50)

# Initialize a local Spark session
spark = (
    SparkSession.builder
    .appName("OrderFlow-Spark-Test")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print(f"\nSpark version: {spark.version}")
    print(f"Python executable: {sys.executable}")

    # Create a small example dataset
    data = [
        (1, "Laptop", 1500.0),
        (2, "Mouse", 25.0),
        (3, "Keyboard", 75.0),
        (4, "Monitor", 350.0),
    ]

    columns = ["product_id", "product_name", "price"]

    df = spark.createDataFrame(data, columns)

    print("\nSample DataFrame:")
    df.show()

    print("\nDataFrame schema:")
    df.printSchema()

    print(f"\nTotal records: {df.count()}")

    print("\nPYSPARK TEST COMPLETED SUCCESSFULLY")

finally:
    spark.stop()
