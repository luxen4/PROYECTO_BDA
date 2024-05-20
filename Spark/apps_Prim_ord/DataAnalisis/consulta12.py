import sessions 


jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

spark = sessions.sesionSpark()


def select1():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT id_reserva, id_habitacion, categoria, tarifa_nocturna FROM tabla_spark; """)

    df_resultado.show()
    spark.stop()
    

# ¿Cuántas reservas se hicieron para cada categoría de habitación?
