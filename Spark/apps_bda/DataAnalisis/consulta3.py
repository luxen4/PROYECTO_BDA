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



def select1():

    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT AVG(DATEDIFF(fecha_salida, fecha_entrada)) AS duracion_media_estancia
                                            FROM tabla_spark;
                                """)
    
    # "¿Cuál es la duración media de la estancia de los clientes de un hotel?
    
    df_resultado.show()
    
def select2():

    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    # Ejecutar la consulta SQL para obtener los clientes y sus preferencias de habitación y comida
    
    df_resultado = spark.sql("""SELECT date_trunc('week', fecha_entrada) AS semana,
                                    COUNT(*) AS num_reservas
                                FROM tabla_spark
                                GROUP BY date_trunc('week', fecha_entrada)
                                ORDER BY num_reservas DESC
                                LIMIT 5;
                                """)

    # Mostrar el resultado de la consulta
    df_resultado.show()
  
def select3():

    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    # Ejecutar la consulta SQL para obtener los clientes y sus preferencias de habitación y comida
    
    df_resultado = spark.sql("""SELECT date_trunc('month', fecha_entrada) AS mes,
                                        COUNT(*) AS num_reservas
                                    FROM tabla_spark
                                    GROUP BY date_trunc('month', fecha_entrada)
                                    ORDER BY num_reservas DESC
                                    LIMIT 5;
                                """)

    # Mostrar el resultado de la consulta
    df_resultado.show()
 
   

print("Media general de todos en todo el registro.")
select1()    
print("Por Semanas")
select2()
print("Media por meses")
select3()

spark.stop()



# "¿Cuál es la duración media de la estancia de los clientes de un hotel?
# "¿Existen periodos de máxima ocupación en función de las fechas de reserva?")




'''


df_resultado = spark.sql("""SELECT fecha_entrada, COUNT(*) AS num_reservas
FROM tabla_spark
GROUP BY fecha_entrada
ORDER BY num_reservas DESC
LIMIT 5;
                             """)



'''

'''
SELECT date_trunc('year', fecha_entrada) AS año,
       COUNT(*) AS num_reservas
FROM reservas_habitaciones
GROUP BY date_trunc('year', fecha_entrada)
ORDER BY num_reservas DESC
LIMIT 5;
'''



'''
SELECT fecha_reserva, COUNT(*) AS num_reservas
FROM (
    SELECT generate_series(min(fecha_entrada), max(fecha_salida), '1 day'::interval) AS fecha_reserva
    FROM tabla_spark
) AS fechas_reserva
JOIN tabla_spark
ON fecha_reserva BETWEEN fecha_entrada AND fecha_salida
GROUP BY fecha_reserva
ORDER BY num_reservas DESC
LIMIT 5; '''



'''
df_resultado = spark.sql("""
        SELECT date_trunc('month', fecha_entrada) AS mes,
            COUNT(*) AS num_reservas
        FROM tabla_spark
        GROUP BY date_trunc('month', fecha_entrada)
        ORDER BY num_reservas DESC
        LIMIT 5;
    """)
'''