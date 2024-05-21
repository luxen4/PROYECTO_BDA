#import sessions 
#spark = sessions.sesionSpark()

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}

def select_Empleados(spark):

    print("¿Quiénes son los empleados que trabajan en cada restaurante, junto con sus cargos y fechas de contratación?")
    
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    df_resultado = spark.sql("""SELECT empleado_name, hotel_name, posicion, fecha_contratación FROM tabla_spark
                                group by empleado_name
                                order by empleado_name asc;
                                """)

    # Mostrar el resultado de la consulta
    df_resultado.show()
    
    
#select1()
#spark.stop()