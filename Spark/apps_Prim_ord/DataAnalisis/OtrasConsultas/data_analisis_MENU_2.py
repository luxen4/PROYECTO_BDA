
from pyspark.sql import SparkSession

def sesionSpark():
    spark = SparkSession.builder \
    .appName("Leer y procesar con Spark") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", 'test') \
    .config("spark.hadoop.fs.s3a.secret.key", 'test') \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/a_jars/hadoop-aws-3.3.1.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark-apps/a_jars/hadoop-aws-3.3.1.jar") \
    .config("spark.jars","/opt/spark-apps/a_jars/postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/a_jars/postgresql-42.7.3.jar") \
    .master("local[*]") \
    .getOrCreate()
    
    return spark

spark=sesionSpark()


def informacion():
    print("Data Warehouse")
    
    print(" \n10. \n" +
            "• ¿Número de reservas por meses?")

    print(" \n11. \n" +
               "• ¿Qué clientes han hecho reservas y cuáles son sus preferencias de habitación y comida?")
    
    print(" \n12. \n" + 
               "• ¿Qué habitaciones hay reservadas para cada reserva, y cuáles son sus respectivas categorías y tarifas nocturnas?")
    
    '''
    print(" \n13. \n" +
               "• ¿Quiénes son los empleados que trabajan en cada restaurante, junto con sus cargos y fechas de contratación?")'''
    
    print(" \n14. \n" +
               "• ¿Cuántas reservas se hicieron para cada categoría de habitación? \n" +
               "• ¿Cuáles son las correspondientes preferencias de comida de los clientes?")
    
    

    print("\n 99. Salir")
    


salir='no'
while salir != 'si':
    
    informacion()
    opcion = int(input("\n Selecciona una opción: \n"))
    
    if opcion == 10:
        import consulta10
        consulta10.select_NumReservasMes(spark)
    
    elif opcion == 11:
        import consulta11
        consulta11.select_ClientesReservas(spark)
    
    elif opcion == 12:
        import consulta12
        consulta12.select_HabitacionesReserva(spark)
    
    elif opcion == 13:
        import consulta13
        consulta13.select_ReservasTipoHabitacion(spark)
        consulta13.select_PreferenciasComidaHabituales(spark)
        
        
    elif opcion == 99:
        print("\n                                               *** Adios *** \n")
        salir="si"
        break
    else:
        print("Opción no válida")
        

    salir = input("\n Quiere salir? \n")


''' Hay que hacer una tabla de empleados para desglosar sos cargos
elif opcion == 15:
    import Spark.apps_Prim_ord.DataAnalisis.OtrasConsultas._consulta13 as _consulta13
    _consulta13.select_Empleados(spark)'''
