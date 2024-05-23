
import sessions
spark = sessions.sesionSpark()


def informacion():
    print("Data Warehouse")

    print("\n 1. Análisis de las preferencias de los clientes\n" +
                "• ¿Cuáles son las preferencias alimenticias más comunes entre los clientes?") 

    print("\n 2. Análisis del rendimiento del restaurante:\n" + 
                "• ¿Qué restaurante tiene el precio medio de menú más alto? \n" +
                "• ¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?")

    print("\n 3. Patrones de reserva \n" +
                "• ¿Cuál es la duración media de la estancia de los clientes de un hotel? \n" +
                "• ¿Existen periodos de máxima ocupación en función de las fechas de reserva?")

    print("\n 4. Gestión de empleados\n" +
                "• ¿Cuántos empleados tiene de media cada hotel?")

    print("\n 5. Ocupación e ingresos del hotel \n" + 
                "• ¿Cuál es el índice de ocupación de cada hotel y varía según la categoría de habitación? \n" + 
                "• ¿Podemos estimar los ingresos generados por cada hotel basándonos en los precios de las habitaciones y los índices de ocupación?")
	
    print("\n 6. Análisis de menús\n" +
                "• ¿Qué platos son los más y los menos populares entre los restaurantes?")

    print("\n 7. Comportamiento de los clientes \n" +
                "• ¿Existen pautas en las preferencias de los clientes en función de la época del año? \n" +
                "• ¿Los clientes con preferencias dietéticas específicas tienden a reservar en restaurantes concretos?")

    print("\n 8. Garantía de calidad \n" +
                "• ¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas reales realizadas?")

    print("\n 9. Análisis de mercado \n" +
                "• ¿Cómo se comparan los precios de las habitaciones de los distintos hoteles y existen valores atípicos?")
    
    
    
    '''
    print("\n 11. ¿Qué clientes han hecho reservas \n" + 
             "y cuáles son sus preferencias de habitación y comida?")
    
    print("\n 12. ¿Qué habitaciones hay reservadas para cada reserva, " + 
          " y cuáles son sus respectivas categorías y tarifas nocturnas?")
    
    
    print("\n 13. ¿Quiénes son los empleados que trabajan en cada restaurante, " + 
          "junto con sus cargos y fechas de contratación?")
    
    print("\n 14.  ¿Cuántas reservas se hicieron para cada categoría de habitación, y " +
               "cuáles son las correspondientes preferencias de comida de los clientes?")'''
    
    

    print("\n 99. Salir")
    


salir='no'
while salir != 'si':
    
    informacion()
    try:
        
        opcion = int(input("\n Selecciona una opción: \n"))
        print(opcion)
        
        if opcion == 1:
            import consulta1
            consulta1.select(spark)
            
        elif opcion == 2:
            import consulta2
            consulta2.select1(spark)
            consulta2.select2(spark)
            
        elif opcion == 3:
            import consulta3
            consulta3.reservas_DuracionMediaTotal(spark)
            consulta3.reservas_porDia(spark)
            consulta3.reservas_PorSemanas(spark)
            consulta3.reservas_porMes(spark)
            consulta3.reservas_porAño(spark)
            
        elif opcion == 4:
            import consulta4
            consulta4.select(spark)
            
        elif opcion == 5:
            import consulta5
            consulta5.select_IndiceOcupacion(spark)
            consulta5.select(spark,'Economica')
            consulta5.select(spark,'Estandar')
            consulta5.select(spark,'Deluxe')
            
            consulta5.select_IngresosGenerados(spark)
            
        elif opcion == 6:
            import consulta6
            consulta6.platosPopulares(spark)
            consulta6.frecuenciaAlergenos(spark)
            consulta6.ingredientesComunes(spark)
        
        elif opcion == 7:
            import consulta7
            consulta7.select0(spark)
            consulta7.select(spark)
            
        elif opcion == 8:
            import consulta8
            consulta8.select(spark)
            consulta8.select2(spark)
            
        elif opcion == 9:
            print("¿Cómo se comparan los precios de las habitaciones de los distintos hoteles existen valores atípicos?")
            import consulta9
            consulta9.init(spark)
        
            
        if opcion == 99:
            print("\n                                               *** Adios *** \n")
            salir="si"
            break
        
    
    except Exception as e: 
        print(e)
        print("opción no válida")    

    salir = input("\n Quiere salir? \n")

