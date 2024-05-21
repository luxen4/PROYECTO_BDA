

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}


def select_NumReservasMes(spark):

    print("¿Número de reservas por meses?")
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT date_trunc('month', fecha_llegada) AS mes, COUNT(*) AS num_reservas FROM tabla_spark
                                    GROUP BY date_trunc('month', fecha_llegada)
                                    ORDER BY num_reservas DESC
                                    LIMIT 5; """)
    df_resultado.show()

# import sessions    
# spark = sessions.sesionSpark()
#   select_NumReservasMes()
# spark.stop()