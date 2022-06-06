import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


if __name__ == "__main__":
	conf = SparkConf().setAll([
            ("spark.hadoop.fs.gs.storage.root.url", "http://storage:8080"),
            ("spark.hadoop.fs.gs.project.id", "test-project"),
            ("spark.hadoop.fs.gs.token.server.url", "http://storage:8080"),
            ("spark.hadoop.google.cloud.auth.service.account.enable", "false"),
            ("spark.hadoop.google.cloud.auth.null.enable", "true"),
            ("spark.master", os.environ.get(
                "SPARK_MASTER_URL", "spark://spark:7077")),
        ])
	spark = SparkSession.builder.appName(
            "ReadGCS").config(conf=conf).getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")
	bucket_name = "my-bucket"
	path = f"gs://{bucket_name}/input/sample.csv"
	df = spark.read.csv(path, header=True)
	df.show()
