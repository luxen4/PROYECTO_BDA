from pyspark.sql import SparkSession
import sessions

spark = sessions.sesionSpark()

def select():

    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT cliente_name, preferencias_alimenticias, restaurante_name FROM tabla_spark; """)


    df_resultado.show()
    spark.stop()
  
  
print("¿Los clientes con preferencias dietéticas específicas tienden a reservar en restaurantes concretos?")
select()


selectZZZZZZ  ()

'''
5.2.8 Garantía de calidad
¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas
reales realizadas?'''
