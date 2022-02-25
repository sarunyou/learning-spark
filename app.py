import os

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

if __name__ == "__main__":
	conf = SparkConf()
	conf.setAll(
			[
					(
							"spark.master",
							os.environ.get("SPARK_MASTER_URL"),
					),
					("spark.app.name", "simple-app"),
			]
	)

	spark = SparkSession.builder.appName("Sample App").config("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark:7077")).getOrCreate()
	rows = [
			[1, 100],
			[2, 200],
			[3, 300],
	]

	schema = StructType(
			[
					StructField("id", IntegerType(), True),
					StructField("salary", IntegerType(), True),
			]
	)

	df = spark.createDataFrame(rows, schema=schema)

	highest_salary = df.agg({"salary": "max"}).collect()[0]["max(salary)"]

	second_highest_salary = (
			df.filter(f"`salary` < {highest_salary}")
			.orderBy("salary", ascending=False)
			.select("salary")
			.limit(1)
	)

	second_highest_salary.show()
	spark.stop()
