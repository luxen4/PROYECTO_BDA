
jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}


   
print("¿Cuál es la duración media de la estancia de los clientes de un hotel)?")


def select1(spark):
    print("Média de los dias de estancia de la gente en los hoteles, en dias")
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    df_resultado = spark.sql("""SELECT AVG(DATEDIFF(fecha_salida, fecha_entrada)) AS duracion_media FROM tabla_spark; """)
    df_resultado.show()
    
    
def select2(spark):
    print("Número de reservas por Semanas")

    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT date_trunc('week', fecha_entrada) AS semana, COUNT(*) AS num_reservas FROM tabla_spark
                                GROUP BY date_trunc('week', fecha_entrada)
                                ORDER BY num_reservas DESC
                                LIMIT 5; """)
    df_resultado.show()
  
  


   
   
   
 
def reservas_porDia(spark):
    print("Número de reservas por dia")
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT fecha_entrada, COUNT(*) AS num_reservas
                                FROM tabla_spark
                                GROUP BY fecha_entrada
                                ORDER BY num_reservas DESC
                                LIMIT 5; """) # dia

    df_resultado.show()
   
def reservas_porMes(spark):
    print("Número de reservas por meses")
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT date_trunc('month', fecha_entrada) AS mes, COUNT(*) AS num_reservas FROM tabla_spark
                                    GROUP BY date_trunc('month', fecha_entrada)  
                                    ORDER BY num_reservas DESC
                                    LIMIT 5; """) # mes

    df_resultado.show()


def reservas_porAño(spark):
    print("Número de reservas por año")
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT date_trunc('year', fecha_entrada) AS año,
                                    COUNT(*) AS num_reservas
                                    FROM reservas_habitaciones
                                    GROUP BY date_trunc('year', fecha_entrada)
                                    ORDER BY num_reservas DESC
                                    LIMIT 5;""") 

    df_resultado.show()


def reservas_Otro(spark):
    print("Número de reservas por año")
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    df_resultado = spark.sql("""SELECT fecha_reserva, COUNT(*) AS num_reservas
                                    FROM (
                                        SELECT generate_series(min(fecha_entrada), max(fecha_salida), '1 day'::interval) AS fecha_reserva
                                        FROM tabla_spark
                                    ) AS fechas_reserva
                                    JOIN tabla_spark
                                    ON fecha_reserva BETWEEN fecha_entrada AND fecha_salida
                                    GROUP BY fecha_reserva
                                    ORDER BY num_reservas DESC
                                    LIMIT 5;""") 

    df_resultado.show()


