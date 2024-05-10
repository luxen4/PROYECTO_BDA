import sessions
jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

spark = sessions.sesionSpark()

def select1():

    df = spark.read.jdbc(url=jdbc_url, table="w_restaurantes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    # Ejecutar la consulta SQL para obtener los clientes y sus preferencias de habitación y comida
    df_resultado = spark.sql("""SELECT restaurante_name, AVG(menu_price) FROM tabla_spark
                                GROUP BY restaurante_name, menu_price
                                ORDER BY menu_price DESC
                                LIMIT 5; """)
    df_resultado.show()


def select2():

    df = spark.read.jdbc(url=jdbc_url, table="w_restaurantes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    # Ejecutar la consulta SQL para obtener los clientes y sus preferencias de habitación y comida
    df_resultado = spark.sql("""SELECT plato_nombre, count(plato_nombre) as cantidad FROM tabla_spark
                                GROUP BY plato_nombre
                                ORDER BY cantidad
                                LIMIT 5; """)
    df_resultado.show()
   
    
    
print("¿Qué restaurante tiene el precio medio de menú más alto?")
select1()

print("¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?")
select2()

spark.stop()