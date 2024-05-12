
jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

# Media en general
def select1(spark):
    print("dias")
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    df_resultado = spark.sql("""SELECT AVG(DATEDIFF(fecha_salida, fecha_entrada)) AS duracion_media FROM tabla_spark; """)
    df_resultado.show()
    
    
# Por semanas    
def select2(spark):
    print("Por Semanas")
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT date_trunc('week', fecha_entrada) AS semana, COUNT(*) AS num_reservas FROM tabla_spark
                                GROUP BY date_trunc('week', fecha_entrada)
                                ORDER BY num_reservas DESC
                                LIMIT 5; """)
    df_resultado.show()
  
  
# Por meses  
def select3(spark):
    print("Media por meses")
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT date_trunc('month', fecha_entrada) AS mes, COUNT(*) AS num_reservas FROM tabla_spark
                                    GROUP BY date_trunc('month', fecha_entrada)  
                                    ORDER BY num_reservas DESC
                                    LIMIT 5; """) # mes

    df_resultado.show()
   
print("¿Cuál es la duración media de la estancia de los clientes de un hotel)?")

# a W_RESERVAS
# "¿Cuál es la duración media de la estancia de los clientes de un hotel?
# "¿Existen periodos de máxima ocupación en función de las fechas de reserva?")




'''


df_resultado = spark.sql("""SELECT fecha_entrada, COUNT(*) AS num_reservas
FROM tabla_spark
GROUP BY fecha_entrada
ORDER BY num_reservas DESC
LIMIT 5;
                             """)



'''

'''
SELECT date_trunc('year', fecha_entrada) AS año,
       COUNT(*) AS num_reservas
FROM reservas_habitaciones
GROUP BY date_trunc('year', fecha_entrada)
ORDER BY num_reservas DESC
LIMIT 5;
'''



'''
SELECT fecha_reserva, COUNT(*) AS num_reservas
FROM (
    SELECT generate_series(min(fecha_entrada), max(fecha_salida), '1 day'::interval) AS fecha_reserva
    FROM tabla_spark
) AS fechas_reserva
JOIN tabla_spark
ON fecha_reserva BETWEEN fecha_entrada AND fecha_salida
GROUP BY fecha_reserva
ORDER BY num_reservas DESC
LIMIT 5; '''



'''
df_resultado = spark.sql("""
        SELECT date_trunc('month', fecha_entrada) AS mes,
            COUNT(*) AS num_reservas
        FROM tabla_spark
        GROUP BY date_trunc('month', fecha_entrada)
        ORDER BY num_reservas DESC
        LIMIT 5;
    """)
'''