# import sessions    
# spark = sessions.sesionSpark()

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}

def select_ClientesReservas(spark):

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    print("¿Qué clientes han hecho reservas y cuáles son sus preferencias de habitación y comida?")
    df_resultado = spark.sql("""SELECT cliente_name, categoria, preferencias_alimenticias FROM tabla_spark;""")

    df_resultado.show()
   
#select1()
#spark.stop()