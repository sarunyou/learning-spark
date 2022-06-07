import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
bucket_name = "example-spark"


def foreach_batch_function(df, epoch_id):
    file_name = df.first()["name"]
    path = f"gs://{bucket_name}/output/{file_name}"
    print("file_name", file_name)
    print("path", path)
    countDistinct = df.groupBy(df.id).count()
    countDistinct.show()

    # df.write.mode("append").csv("/opt/bitnami/spark/files")
    # df.select(df.id).coalesce(1).write.mode("append").csv(path)
    countDistinct.select(df.id).write.mode("overwrite").csv(path)
    # df.saveAsTextFile(path)


if __name__ == "__main__":
    path = f"gs://{bucket_name}/input/"
    checkpoint_path = f"gs://{bucket_name}/checkpoint/"
    schema = StructType().add("id", "string")
    conf = SparkConf().setAll([
        ("fs.gs.auth.service.account.json.keyfile", "/test-files/key.json"),
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
    # countDistinct = csvDf

    csvDf.writeStream.format("console").option("checkpointLocation", checkpoint_path).foreachBatch(foreach_batch_function).outputMode(
        "update").start().awaitTermination()
    # countDistinct.writeStream.format("csv").option("path", "/")
# response = requests.get('http://server:8000')
# print("response", response.text())
# spark.stop()
