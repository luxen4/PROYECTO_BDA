import sessions

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

spark = sessions.sesionSpark()

def select1():
    
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT plato_name, count(plato name) FROM tabla_spark; """)
    df_resultado.show()
    
    
def select2():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT alergenos, count(plato name) FROM tabla_spark;""")
    df_resultado.show()
    
    
print("5.2.6 Análisis de menús")  
 
print("¿Qué platos son los más y los menos populares entre los restaurantes?")
select1()

print("¿Hay ingredientes o alérgenos comunes que aparezcan con frecuencia en los platos?")
select2()

spark.stop()

# ¿Cuántas reservas se hicieron para cada categoría de habitación?



