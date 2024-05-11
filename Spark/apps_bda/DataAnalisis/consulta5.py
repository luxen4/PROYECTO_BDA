import sessions
spark = sessions.sesionSpark()

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}


def select1():
   
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT hotel_name, count(reserva_id) FROM tabla_spark group by hotel_name; """)
    df_resultado.show()
    
    
def select2():

    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT hotel_name, categoria_habitacion, count(reserva_id) as numero_de_reservas FROM tabla_spark 
                             group by categoria_habitacion, hotel_name
                             order by categoria_habitacion, hotel_name desc
                             """)
    df_resultado.show()


def select3():
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT hotel_name, SUM(price_habitacion) as cuantia FROM tabla_spark
                                 group by hotel_name
                                 order by cuantia desc
                             ; """)
    df_resultado.show()
    
    
    

def select10():

    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT hotel_name, SUM(price_habitacion) as cuantia FROM tabla_spark
                                 group by hotel_name
                                 order by cuantia desc
                             ; """)
    df_resultado.show()


def select4():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT SUM(tarifa_por_noche), hotel_name FROM tabla_spark group by hotel_name ;""")
    df_resultado.show()
    
    
    
print("¿Cuál es el índice de ocupación de cada hotel?")
#select1()

print("¿Cuál es el índice de ocupación de cada hotel según la categoría de habitación?")
#select2()

print("¿Cuál es el índice de ocupación de cada hotel según la categoría de habitación?")
select3()


spark.stop()



# ¿Cuántas reservas se hicieron para cada categoría de habitación?

# "5.2.5 Ocupación e ingresos del hotel \n" 
# "¿Podemos estimar los ingresos generados por cada hotel basándonos en los \n" +
# "precios de las habitaciones y los índices de ocupación?"
