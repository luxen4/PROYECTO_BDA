jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}


def platosPopulares(spark):
    print("¿Qué platos son los más y los menos populares entre los restaurantes?")
    df = spark.read.jdbc(url=jdbc_url, table="w_restaurantes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT nombre, count(nombre) as veces FROM tabla_spark
                                    group by nombre
                                    order by veces desc ; """)
    df_resultado.show()
    
    
def frecuenciaAlergenos(spark):
    print("¿Hay alérgenos comunes que aparezcan con frecuencia en los platos?")
    df = spark.read.jdbc(url=jdbc_url, table="w_restaurantes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT alergenos, count(alergenos) as veces FROM tabla_spark
                                group by alergenos
                                order by veces desc ;""")
    df_resultado.show()


def ingredientesComunes(spark):
    print("¿Hay ingredientes comunes que aparezcan con frecuencia en los platos?")
    df = spark.read.jdbc(url=jdbc_url, table="w_restaurantes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT ingredientes, count(ingredientes) as veces FROM tabla_spark
                                group by ingredientes
                                order by veces desc ;""")
    df_resultado.show()


    
print("5.2.6 Análisis de menús")




