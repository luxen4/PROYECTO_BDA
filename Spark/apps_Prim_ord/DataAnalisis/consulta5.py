#import sessions
#spark = sessions.sesionSpark()

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}


def select_IndiceOcupacion(spark):
    print("¿Cuál es el índice de ocupación de cada hotel?")
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT nombre_hotel, count(id_reserva) as num_reservas FROM tabla_spark 
                             group by nombre_hotel
                             order by num_reservas desc
                             ; """)
    df_resultado.show()
    
    
    
    
def select(spark, categoria):
    print("¿Cuál es el índice de ocupación de cada hotel según la categória de habitación " + categoria + "?")
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    df_resultado = spark.sql("""SELECT nombre_hotel, count(id_reserva) as num_reservas FROM tabla_spark 
                             where categoria= '""" + categoria +
                             """' group by nombre_hotel
                             order by num_reservas desc; 
                             """)
    df_resultado.show()
    
    
    
def select_IngresosGenerados(spark):
    print("¿Podemos estimar los ingresos generados por cada hotel basándonos en los \n" +
           "precios de las habitaciones y los índices de ocupación?")
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    #df_resultado = spark.sql("""SELECT SUM(tarifa_por_noche), hotel_name FROM tabla_spark group by hotel_name ;""")
    
   
    df_resultado = spark.sql(""" SELECT nombre_hotel, ROUND(SUM(tarifa_por_noche), 2) as cuantia FROM tabla_spark 
                                    GROUP BY nombre_hotel
                                    ORDER BY cuantia desc; """)
    df_resultado.show()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
#print("¿Cuál es el índice de ocupación de cada hotel?")
#select1()

#print("¿Cuál es el índice de ocupación de cada hotel según la categoría de habitación?")
#select2()

#print("¿Cuál es el índice de ocupación de cada hotel según la categoría de habitación?")
#select3(spark)


#spark.stop()



# ¿Cuántas reservas se hicieron para cada categoría de habitación?

# "5.2.5 Ocupación e ingresos del hotel \n" 
# "¿Podemos estimar los ingresos generados por cada hotel basándonos en los \n" +
# "precios de las habitaciones y los índices de ocupación?"
