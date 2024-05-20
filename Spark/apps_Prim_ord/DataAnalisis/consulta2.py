
jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}

def select1(spark):
    print("¿Qué restaurante tiene el precio medio de menú más alto?")
    df = spark.read.jdbc(url=jdbc_url, table="w_restaurantes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    # Ejecutar la consulta SQL para obtener los clientes y sus preferencias de habitación y comida
    df_resultado = spark.sql("""SELECT restaurante_name, AVG(precio) FROM tabla_spark
                                GROUP BY restaurante_name, precio
                                ORDER BY precio DESC
                                LIMIT 5; """)
    df_resultado.show()



def select2(spark):
    print("¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?")
    df = spark.read.jdbc(url=jdbc_url, table="w_restaurantes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    # Ejecutar la consulta SQL para obtener los clientes y sus preferencias de habitación y comida
    df_resultado = spark.sql("""SELECT nombre, count(nombre) as cantidad FROM tabla_spark
                                GROUP BY nombre
                                ORDER BY cantidad desc
                                LIMIT 5; """)
    df_resultado.show()

