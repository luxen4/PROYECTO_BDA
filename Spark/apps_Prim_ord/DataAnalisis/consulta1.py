jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}


def select(spark):
    print("¿Cuáles son las preferencias alimenticias más comunes entre los clientes?") 
    df = spark.read.jdbc(url=jdbc_url, table="w_clientes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT nombre_cliente, preferencias_comida FROM tabla_spark """)
    df_resultado.show()
    