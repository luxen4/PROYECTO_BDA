#from pyspark.sql import SparkSession
#import sessions

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}

#spark = sessions.sesionSpark()

def select(spark,categoria,orden):
    print(categoria)
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql(f"""
                                SELECT nombre_hotel, tarifa_por_noche 
                                FROM tabla_spark 
                                WHERE categoria = '{categoria}'
                                ORDER BY tarifa_por_noche {orden}
                            """)

    df_resultado.show()
    
def init(spark):    
    categoria='Deluxe'
    select(spark, categoria,'asc')
    select(spark, categoria,'desc')

    categoria='Estandar'
    select(spark, categoria,'asc')
    select(spark, categoria,'desc')

    categoria='Economica'
    select(spark, categoria,'asc')
    select(spark, categoria,'desc')    


#spark.stop()
'''
'''


'''
habitaciones
numero_habitacion,categoria,tarifa_por_noche
1,Deluxe,406.57                                                 
2,Estandar,472.84
3,Economica,82.01
'''