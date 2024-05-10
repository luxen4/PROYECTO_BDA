from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("Leer y procesar con Spark") \
.config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
.config("spark.hadoop.fs.s3a.access.key", 'test') \
.config("spark.hadoop.fs.s3a.secret.key", 'test') \
.config("spark.sql.shuffle.partitions", "4") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
.config("spark.hadoop.fs.s3a.path.style.access", "true") \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
.config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
.config("spark.jars","./postgresql-42.7.3.jar") \
.config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
.master("local[*]") \
.getOrCreate()

bucket_name = 'my-local-bucket' 
file_name = 'data_hoteles.json'
df_hoteles= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
df_hoteles.show()


 # json
ruta_salida = "./hotels_data.json"
df_hoteles.write.option("multiline", "true").json(ruta_salida, mode="overwrite")
