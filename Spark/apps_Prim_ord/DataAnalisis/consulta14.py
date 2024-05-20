import sessions 

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

spark = sessions.sesionSpark()


def select1():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT tipo_habitacion, count(id_reserva) FROM tabla_spark
                                 group by tipo_habitacion;  """)

    df_resultado.show()
    

def select2():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    # Ejecutar la consulta SQL para obtener los clientes y sus preferencias de habitación y comida
    
    df_resultado = spark.sql("""SELECT  preferencias_comida, COUNT(preferencias_comida) FROM tabla_spark
                                GROUP BY preferencias_comida
                                ORDER BY num_reservas DESC; """)

    df_resultado.show()



def select3():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    # Ejecutar la consulta SQL para obtener los clientes y sus preferencias de habitación y comida
    
    df_resultado = spark.sql("""SELECT date_trunc('month', fecha_entrada) AS mes, COUNT(*) AS num_reservas FROM tabla_spark
                                    GROUP BY date_trunc('month', fecha_entrada)
                                    ORDER BY num_reservas DESC
                                    LIMIT 5; """)

    df_resultado.show()

   

print("14_1 ¿Cuántas reservas se hicieron para cada categoría de habitación?")
select1()    

print("14_2 ¿Cuáles son las correspondientes preferencias de comida de los clientes?")
select2()    

spark.stop()