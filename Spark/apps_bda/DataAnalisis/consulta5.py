import sessions
spark = sessions.sesionSpark()

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}


def select1():
   
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT hotel_name, count(id_reservas) FROM tabla_spark group by hotel_name; """)
    df_resultado.show()
    
    
def select2():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT categoria, count(id_reservas) FROM tabla_spark; """)
    df_resultado.show()


def select3():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT SUM(tarifa_nocturna), hotel_name FROM tabla_spark; """)
    df_resultado.show()


def select4():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT SUM(tarifa_nocturna), hotel_name FROM tabla_spark group by hotel_name ;""")
    df_resultado.show()
    
    
    
print("¿Cuál es el índice de ocupación de cada hotel?")
select1()

print("¿Cuál es el índice de ocupación de cada hotel según la categoría de habitación?")
select2()

print("¿Cuál es el índice de ocupación de cada hotel según la categoría de habitación?")
select3()


spark.stop()



# ¿Cuántas reservas se hicieron para cada categoría de habitación?

# "5.2.5 Ocupación e ingresos del hotel \n" 
# "¿Podemos estimar los ingresos generados por cada hotel basándonos en los \n" +
# "precios de las habitaciones y los índices de ocupación?"
