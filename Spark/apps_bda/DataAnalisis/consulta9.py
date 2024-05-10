from pyspark.sql import SparkSession
import sessions

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

spark = sessions.sesionSpark()

def select(categoria):
    
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(""" SELECT hotel_name, numero_habitacion, tarifa_nocturna FROM tabla_spark where categoria='Deluxe'; """)

    df_resultado.show()
    spark.stop()
    
    
print("¿Cómo se comparan los precios de las habitaciones de los distintos hoteles existen valores atípicos?")
categoria='Deluxe'
select(categoria)


'''
habitaciones
numero_habitacion,categoria,tarifa_por_noche
1,Deluxe,406.57                                                 Buscar las habitaciones deluxe
2,Estandar,472.84
3,Economica,82.01

hoteles
id_hotel,nombre_hotel,direccion_hotel,empleados
1,Brown PLC Hotel,"73161 Samuel Extensions Suite 563
'''