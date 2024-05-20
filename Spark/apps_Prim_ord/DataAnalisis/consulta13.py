import sessions 


jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

spark = sessions.sesionSpark()


def select1():

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")


    df_resultado = spark.sql("""SELECT empleado_name, hotel_name, posicion, fecha_contratación FROM tabla_spark
                                group by empleado_name
                                order by empleado_name asc;
                                """)

    # Mostrar el resultado de la consulta
    df_resultado.show()
    
    
select1()
spark.stop()