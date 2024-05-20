jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}

def select(spark):
    print("¿Los clientes con preferencias dietéticas específicas tienden a reservar en restaurantes concretos?")


    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT cliente_name, preferencias_alimenticias, restaurante_name FROM tabla_spark
                             
                             order by cliente_name
                             ; """)

    df_resultado.show(100)
    
    
def select2(spark):
    print("¿Los clientes con preferencias dietéticas específicas tienden a reservar en restaurantes concretos?")


    df = spark.read.jdbc(url=jdbc_url, table="w_clientes", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT nombre_cliente, preferencias_alimenticias FROM tabla_spark
                             order by nombre_cliente
                             
                             ; """)

    df_resultado.show(100)
  
  

