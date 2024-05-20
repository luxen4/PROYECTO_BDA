#from pyspark.sql import SparkSession
#import sessions

#spark = sessions.sesionSpark()

def select(spark):
    print("¿Los clientes con preferencias dietéticas específicas tienden a reservar en restaurantes concretos?")
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT cliente_name, preferencia_comida, restaurante_name FROM tabla_spark
                                order by cliente_name; """)

    df_resultado.show()
  
  
#print("¿Los clientes con preferencias dietéticas específicas tienden a reservar en restaurantes concretos?")
#select()
