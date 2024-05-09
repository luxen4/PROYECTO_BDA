import sessions

def select():
    spark = sessions.sesionSpark()

    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="w_clientes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    # Ejecutar la consulta SQL para obtener los clientes y sus preferencias de habitación y comida
    df_resultado = spark.sql(""" SELECT nombre_cliente, preferencias_comida FROM tabla_spark """)

    print("¿Cuáles son las preferencias alimenticias más comunes entre los clientes?")
    df_resultado.show()
    spark.stop()
    
select()
