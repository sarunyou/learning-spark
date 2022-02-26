import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

if __name__ == "__main__":
	logFile = "/test-files/mock.csv"  # Should be some file on your system
	spark = SparkSession.builder.appName("UniqueUsers").config("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark:7077")).getOrCreate()
	logData = spark.read.text(logFile).cache()
	countDistinct = logData.distinct().count()
	print("countDistinct", countDistinct)
	spark.stop()
