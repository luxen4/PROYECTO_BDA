# import sessions 
# spark = sessions.sesionSpark()

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}

def select_ReservasTipoHabitacion(spark):
    print(" ¿Cuántas reservas se hicieron para cada categoría de habitación?")
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    df_resultado = spark.sql(""" SELECT tipo_habitacion, count(id_reserva) FROM tabla_spark
                                 group by tipo_habitacion;  """)
    df_resultado.show()
    

def select_PreferenciasComidaHabituales(spark):

    print(" ¿Cuáles son las correspondientes preferencias de comida de los clientes?")
    df = spark.read.jdbc(url=jdbc_url, table="w_clientes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    df_resultado = spark.sql("""SELECT  preferencias_comida, COUNT(preferencias_comida) as num_reservas FROM tabla_spark
                                GROUP BY preferencias_comida
                                ORDER BY num_reservas DESC; """)
    df_resultado.show()



   

#spark.stop()