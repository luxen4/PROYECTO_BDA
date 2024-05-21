# import sessions 
# spark = sessions.sesionSpark()

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}

def select_HabitacionesReserva(spark):
    
    print("¿Qué habitaciones hay reservadas para cada reserva, y cuáles son sus respectivas categorías y tarifas nocturnas?")
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    df_resultado = spark.sql("""SELECT id_reserva, habitacion_id, categoria, tarifa_por_noche FROM tabla_spark; """)

    df_resultado.show()
 
    
#spark.stop()