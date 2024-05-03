from pyspark.sql import SparkSession

# Crear la sesión de Spark
def sesionSpark():
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
    .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
    .master("local[*]") \
    .getOrCreate()
    
    return spark

# Meter directamente los archivos a S3
try:
    spark=sesionSpark()
    
    # csv
    df = spark.read.csv("./../data_Prim_ord/csv/habitaciones.csv")
    ruta_salida = "s3a://my-local-bucket/habitaciones.csv"
    df=df.write.csv(ruta_salida, mode="overwrite")

    # json
    df = spark.read.option("multiline", "true").json("./../data_Prim_ord/json/restaurantes.json")
    ruta_salida = "s3a://my-local-bucket/restaurantes.json"
    df.write.option("multiline", "true").json(ruta_salida, mode="overwrite")
    
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)




# Crear el bucket por comando
#   awslocal s3api create-bucket --bucket my-local-bucket

# Listar los archivos del bucket
#   awslocal s3 ls s3://my-local-bucket