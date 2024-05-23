
jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}


def select0(spark):
    print("¿Existen pautas en las preferencias de los clientes en función de la época del año?")
  
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT MONTH(fecha_llegada) as mes, preferencias_alimenticias, count(preferencias_alimenticias) as veces FROM tabla_spark
                                GROUP BY MONTH(fecha_llegada), preferencias_alimenticias
                                ORDER BY mes, veces """)
    df_resultado.show(100)



def select(spark):
    print("¿Los clientes con preferencias dietéticas específicas tienden a reservar en restaurantes concretos?")

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT cliente_name, preferencias_alimenticias, restaurante_name FROM tabla_spark
                                group by cliente_name, preferencias_alimenticias, restaurante_name
                                order by cliente_name; """)
    df_resultado.show(100)