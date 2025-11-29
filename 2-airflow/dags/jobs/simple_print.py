from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AirflowSparkTest").getOrCreate()

print("Hello from Spark on Kubernetes!")
print("This is a simple test job.")
print("🎉 Spark Operator test via Airflow was successful!")

spark.stop()
