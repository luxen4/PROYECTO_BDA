#import sessions
#spark = sessions.sesionSpark()

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}


def select1(spark):
    print("¿Cuál es el índice de ocupación de cada hotel?")
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT nombre_hotel, count(id_reserva) as num_reservas FROM tabla_spark group by nombre_hotel; """)
    df_resultado.show()
    
    
def select11(spark):
    print("¿Cuál es el índice de ocupación de cada hotel según la categória de habitación Económica?")
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT nombre_hotel, count(id_reserva) as num_reservas FROM tabla_spark where categoria='Economica' group by nombre_hotel; """)
    df_resultado.show()
    
    
def select111(spark):
    print("¿Cuál es el índice de ocupación de cada hotel según la categória de habitación Estandar?")
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT nombre_hotel, count(id_reserva) as num_reservas FROM tabla_spark where categoria='Estandar' group by nombre_hotel; """)
    df_resultado.show()
    
def select1111(spark):
    print("¿Cuál es el índice de ocupación de cada hotel según la categória de habitación Deluxe?")
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT nombre_hotel, count(id_reserva) as num_reservas FROM tabla_spark where categoria='Deluxe' group by nombre_hotel; """)
    df_resultado.show()    
    
    
    
    
    
def select2(spark):
    print("¿Cuál es el índice de ocupación de cada hotel según la categoría de habitación?")
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT hotel_name, categoria_habitacion, count(reserva_id) as numero_de_reservas FROM tabla_spark 
                             group by categoria_habitacion, hotel_name
                             order by categoria_habitacion, hotel_name desc
                             """)
    df_resultado.show()


def select3(spark):
    print("")
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT hotel_name, SUM(price_habitacion) as cuantia FROM tabla_spark
                                 group by hotel_name
                                 order by cuantia desc
                             ; """)
    df_resultado.show()
    
    
    

def select4(spark):
    print("¿Podemos estimar los ingresos generados por cada hotel basándonos en los \n" +
           "precios de las habitaciones y los índices de ocupación?")
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT SUM(tarifa_por_noche), hotel_name FROM tabla_spark group by hotel_name ;""")
    df_resultado.show()
    
    
    
#print("¿Cuál es el índice de ocupación de cada hotel?")
#select1()

#print("¿Cuál es el índice de ocupación de cada hotel según la categoría de habitación?")
#select2()

print("¿Cuál es el índice de ocupación de cada hotel según la categoría de habitación?")
#select3(spark)


#spark.stop()



# ¿Cuántas reservas se hicieron para cada categoría de habitación?

# "5.2.5 Ocupación e ingresos del hotel \n" 
# "¿Podemos estimar los ingresos generados por cada hotel basándonos en los \n" +
# "precios de las habitaciones y los índices de ocupación?"
