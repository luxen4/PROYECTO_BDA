from pyspark.sql import SparkSession
import sessions

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

spark = sessions.sesionSpark()

def select():
    
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT hotel_name, numero_habitacion, tarifa_nocturna FROM tabla_spark; """)

    df_resultado.show()
    spark.stop()
    
    
print("¿Cómo se comparan los precios de las habitaciones de los distintos hoteles existen valores atípicos?")
select()
