from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType,LongType

spark = SparkSession.builder \
.appName("Leer y procesar con Spark") \
.config("spark.streaming.stopGracefullyOnShutdown", True) \
.config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
.config("spark.hadoop.fs.s3a.access.key", 'test') \
.config("spark.hadoop.fs.s3a.secret.key", 'test') \
.config("spark.sql.shuffle.partitions", "4") \
.config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
.config("spark.hadoop.fs.s3a.path.style.access", "true") \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
.config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
.master("spark://spark-master:7077") \
.getOrCreate()



'''
df =spark  \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9093") \
  .option("subscribe", "info") \
  .option("failOnDataLoss",'false') \
  .load()'''
  
  
# Rafa  
df =spark  \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9093") \
  .option("subscribe", "info") \
  .option("failOnDataLoss",'false') \
  .load()
  
schema = StructType() \
    .add("id_cliente", StringType()) \
    .add("nombre", StringType()) \
    .add("direccion", StringType()) \
    .add("preferencias_alimenticias", StringType())
     

# Convert value column to JSON and apply schema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")


query = df \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "s3a://my-local-bucket/data_clientes") \
    .option("checkpointLocation", "s3a://my-local-bucket/clientes")\
    .option("multiline", "true")\
    .start()

'''
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
'''
df.printSchema()

# Wait for the termination of the querypython 
query.awaitTermination()















































''' Para local
query = df \
    .writeStream \
    .format("json") \
    .option("failOnDataLoss",'false') \
    .option("path", "./datoss") \
    .option("checkpointLocation", "./checkopoint") \
    .start()'''

'''
query = df \
    .writeStream \
    .format("csv") \
    .option("failOnDataLoss",'false') \
    .option("path", "s3a://my-local-bucket/data_reservas.csv") \
    .option("checkpointLocation", "checkpoint_dir") \
    .start()'''
    
'''
query = df.writeStream \
      .format("csv") \
      .option("sep", ",") \
      .option("header", "true") \
      .option("path", "./pruebas") \
      .option("checkpointLocation", "./checkpoint_dir") \
      .start()
'''    