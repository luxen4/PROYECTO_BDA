# Hacer un menu
import sessions

spark = sessions.sesionSpark()


def informacion():
    print("Data Warehouse")

    print("\n 1. ¿Cuáles son las preferencias alimenticias más comunes entre los clientes?") 

    print("\n 2. ¿Qué restaurante tiene el precio medio de menú más alto? \n" +
            "    ¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?")

    print("\n 3. ¿Cuál es la duración media de la estancia de los clientes de un hotel? \n" +
            "    ¿Existen periodos de máxima ocupación en función de las fechas de reserva?")

    print("\n 4. ¿Cuántos empleados tiene de media cada hotel?")

    print("\n 5. Ocupación e ingresos del hotel \n" + 
            "    ¿Cuál es el índice de ocupación de cada hotel y varía según la categoría de habitación? \n" + 
            "    ¿Podemos estimar los ingresos generados por cada hotel basándonos en los \n" +
            "     precios de las habitaciones y los índices de ocupación?")

    print("\n 6. Análisis de menús" +
                "¿Qué platos son los más y los menos populares entre los restaurantes?")

    print("\n 7. Comportamiento de los clientes " +
                "¿Existen pautas en las preferencias de los clientes en función de la época del año? " +
                "¿Los clientes con preferencias dietéticas específicas tienden a reservar en" + 
                "restaurantes concretos?")

    print("\n 8. Garantía de calidad " +
                "¿Existen discrepancias entre la disponibilidad de platos comunicada \n y las reservas " +
                "reales realizadas?")

    print("\n 9. Análisis de mercado " +
            "¿Cómo se comparan los precios de las habitaciones de los distintos hoteles y " +
            "existen valores atípicos?")
    
    
    print("\n 11. ¿Qué clientes han hecho reservas " + 
             "y cuáles son sus preferencias de habitación y comida?")
    
    print("\n 12. ¿Qué habitaciones hay reservadas para cada reserva, " + 
          " y cuáles son sus respectivas categorías y tarifas nocturnas?")
    
    
    print("\n 13. ¿Quiénes son los empleados que trabajan en cada restaurante, " + 
          "junto con sus cargos y fechas de contratación?")
    
    print("\n 14.  ¿Cuántas reservas se hicieron para cada categoría de habitación, y " +
               "cuáles son las correspondientes preferencias de comida de los clientes?")
    
    

    print("\n 10. Salir")
    




salir='no'
while salir != 'si':
    
    informacion()
    opcion = int(input("\n Selecciona una opción: \n"))
    
    if opcion == 1:
        import consulta1
        consulta1.select(spark)
        
    elif opcion == 2:
        import consulta2
        consulta2.select1(spark)
        consulta2.select2(spark)
        
    elif opcion == 3:
        import consulta3
        consulta3.select1(spark)
        consulta3.select2(spark)
        consulta3.select3(spark)
        
    elif opcion == 4:
        import consulta4
        consulta4.select(spark)
        
    if opcion == 5:
        print()
    elif opcion == 6:
        import consulta6
        consulta6.select1(spark)
        consulta6.select2(spark)
        consulta6.select3(spark)
    elif opcion == 7:
        import consulta7
        consulta7.select(spark)
    elif opcion == 8:
        import consulta8
        consulta8.select(spark)
    elif opcion == 9:
        print("¿Cómo se comparan los precios de las habitaciones de los distintos hoteles existen valores atípicos?")
        import consulta9
        consulta9.init()
       
        
    elif opcion == 10:
        print("\n                                               *** Adios *** \n")
        salir="si"
        break
    else:
        print("Opción no válida")
        

    salir = input("\n Quiere salir? \n")


'''
5.2.1 Análisis de las preferencias de los clientes
¿Cuáles son las preferencias alimenticias más comunes entre los clientes?
5.2.2 Análisis del rendimiento del restaurante:
¿Qué restaurante tiene el precio medio de menú más alto?
¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?
5.2.3 Patrones de reserva
¿Cuál es la duración media de la estancia de los clientes de un hotel?
¿Existen periodos de máxima ocupación en función de las fechas de reserva?
5.2.4 Gestión de empleados
¿Cuántos empleados tiene de media cada hotel?
5.2.5 Ocupación e ingresos del hotel
¿Cuál es el índice de ocupación de cada hotel y varía según la categoría de
habitación?
¿Podemos estimar los ingresos generados por cada hotel basándonos en los
precios de las habitaciones y los índices de ocupación?
5.2.6 Análisis de menús
¿Qué platos son los más y los menos populares entre los restaurantes?
23/24 - IABD - Big Data Aplicado
¿Hay ingredientes o alérgenos comunes que aparezcan con frecuencia en los
platos?
5.2.7 Comportamiento de los clientes
¿Existen pautas en las preferencias de los clientes en función de la época del año?
¿Los clientes con preferencias dietéticas específicas tienden a reservar en
restaurantes concretos?
5.2.8 Garantía de calidad
¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas
reales realizadas?
5.2.9 Análisis de mercado
¿Cómo se comparan los precios de las habitaciones de los distintos hoteles y
existen valores atípicos?'''

# # ¿Cuántas reservas se hicieron para cada categoría de habitación?



'''
consultar en clientes
consultar en reservas



selectZZZZZZ  ()

# "¿Existen pautas en las preferencias de los clientes en función de la época del año?
#- Será hoteles preferidos en épocas por mes/o año



5.2.8 Garantía de calidad
¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas
reales realizadas?
'''
