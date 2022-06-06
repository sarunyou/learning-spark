import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql import functions as F


def foreach_batch_function(df, epoch_id):
    file_name = df.first()["name"]
    path = f"gs://{bucket_name}/output/{file_name}"
    print("file_name", file_name)
    print("path", path)
    df.show()

    # df.write.mode("append").csv("/opt/bitnami/spark/files")
    df.select(df.id).coalesce(1).write.mode("append").csv(path)
    # df.saveAsTextFile(path)


if __name__ == "__main__":
    bucket_name = "my-bucket"
    path = f"gs://{bucket_name}/input/"
    schema = StructType().add("id", "string")
    conf = SparkConf().setAll([
        ("spark.hadoop.fs.gs.storage.root.url", "http://storage:8080"),
        ("spark.hadoop.fs.gs.project.id", "test-project"),
        ("spark.hadoop.fs.gs.token.server.url", "http://storage:8080"),
        ("spark.hadoop.google.cloud.auth.service.account.enable", "false"),
        ("spark.hadoop.google.cloud.auth.null.enable", "true"),
        ("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark:7077"))
    ])
    spark = SparkSession.builder.appName(
        "UniqueUsers").config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    csvDf = spark.readStream.option("sep", ";").schema(
        schema).option("maxFilesPerTrigger", 1).csv(path)
    csvDf = csvDf.withColumn("path", F.input_file_name())
    csvDf = csvDf.withColumn("path_splitted", F.split("path", "/"))
    csvDf = csvDf.withColumn("name", F.col(
        "path_splitted").getItem(F.size("path_splitted")-1))
    # csvDf = csvDf.dropDuplicates()
    # countDistinct = csvDf.groupBy(csvDf.id).count().select(csvDf.id)
    # csvDf.show()
    # countDistinct = csvDf.groupBy(csvDf.id).count()
    countDistinct = csvDf

    countDistinct.writeStream.format("console").foreachBatch(foreach_batch_function).outputMode(
        "update").start().awaitTermination()
    # countDistinct.writeStream.format("csv").option("path", "/")
# response = requests.get('http://server:8000')
# print("response", response.text())
# spark.stop()
