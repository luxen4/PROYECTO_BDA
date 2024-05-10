import sessions    

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

spark = sessions.sesionSpark()

def select1():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")


    df_resultado = spark.sql("""SELECT cliente_name, categoria, preferencias_comida FROM tabla_spark;""")

    df_resultado.show()
    spark.stop()
    
    
print("De cada Cliente, categoría de habitación y Preferencias de comida")
select1()


# ¿Cuántas reservas se hicieron para cada categoría de habitación?