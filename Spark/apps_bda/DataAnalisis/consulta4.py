from pyspark.sql import SparkSession

def select():

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


    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    
    # Crear una tabla temporal para descomponer la lista de empleados en filas separadas
    spark.sql("""
        CREATE TEMP VIEW temp_empleados AS
        SELECT nombre_hotel, EXPLODE(SPLIT(empleados, ',')) AS num_empleados
        FROM tabla_spark
    """)

    # Calcular la media de los empleados por hotel
    media_empleados = spark.sql("""
        SELECT nombre_hotel, count(DISTINCT num_empleados) AS media_empleados
        FROM temp_empleados
        GROUP BY nombre_hotel
    """)
    
    print("¿Cuántos empleados tiene de media cada hotel?")
    
    media_empleados.show()
    spark.stop()
    
select()



