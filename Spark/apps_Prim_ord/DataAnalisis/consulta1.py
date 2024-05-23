jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}

'''
def select(spark):
    print("¿Cuáles son las preferencias alimenticias más comunes entre los clientes?") 
    df = spark.read.jdbc(url=jdbc_url, table="w_clientes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT DISTINCT nombre_cliente, preferencias_comida FROM tabla_spark """)
    df_resultado.show()
'''
    
    
def select(spark):
    print("¿Cuáles son las preferencias alimenticias más comunes entre los clientes?") 
    df = spark.read.jdbc(url=jdbc_url, table="w_clientes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT preferencias_comida, count(preferencias_comida) as veces FROM tabla_spark 
                             group by preferencias_comida
                             order by veces desc
                             limit 5
                             """)
    df_resultado.show()