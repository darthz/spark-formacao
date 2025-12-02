"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-2.py
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("learning-spark") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")
print(f"Application Id: {spark.sparkContext.applicationId}")
print(f"Application Name: {spark.sparkContext.appName}")
print(f"Avaiable cores: {spark.sparkContext.defaultParallelism}")


print("\n Active Spark Configs: ")
for item in sorted(spark.sparkContext.getConf().getAll()):
    print(f"{item[0]} : {item[1]}")

spark.stop()