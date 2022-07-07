import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import functions as F


def foreach_batch_function(df, epoch_id):
    file_name = df.first()["name"]
    print("file_name", file_name)
    df = df.dropDuplicates(["id"])
    df.select(["id", "fId"]).show()
if __name__ == "__main__":
    path = f"/test-files/input"
    schema = StructType().add("id", "string").add("fId", "string")
    conf = SparkConf().setAll([
        ("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark:7077"))
    ])
    spark = SparkSession.builder.appName(
        "UniqueUsers").config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    csvDf = spark.readStream.option("sep", ",").schema(
        schema).option("maxFilesPerTrigger", 1).csv(path)
    csvDf = csvDf.withColumn("path", F.input_file_name())
    csvDf = csvDf.withColumn("path_splitted", F.split("path", "/"))
    csvDf = csvDf.withColumn("name", F.col(
        "path_splitted").getItem(F.size("path_splitted")-1))
    csvDf.writeStream.format("console").foreachBatch(foreach_batch_function).outputMode(
        "update").start().awaitTermination()
