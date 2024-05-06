# Hacer un menu

def opcion1():
    print("5.2.1 Análisis de las preferencias de los clientes \n" +
            "¿Cuáles son las preferencias alimenticias más comunes entre los clientes?")

def opcion2():
    print("5.2.2 Análisis del rendimiento del restaurante:" +
            "¿Qué restaurante tiene el precio medio de menú más alto? \n" +
            "¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?")

def opcion3():
    print("5.2.3 Patrones de reserva \n" +
            "¿Cuál es la duración media de la estancia de los clientes de un hotel? \n" +
            "¿Existen periodos de máxima ocupación en función de las fechas de reserva?")

def opcion4():
    print("5.2.4 Gestión de empleados \n" +
            "¿Cuántos empleados tiene de media cada hotel?")
    
    
def opcion5():
    print("5.2.5 Ocupación e ingresos del hotel \n" +
            "¿Cuál es el índice de ocupación de cada hotel y varía según la categoría de habitación?" +
            "¿Podemos estimar los ingresos generados por cada hotel basándonos en los \n" +
            "precios de las habitaciones y los índices de ocupación?")
    
def opcion6():
    print("5.2.6 Análisis de menús \n" +
            "¿Qué platos son los más y los menos populares entre los restaurantes?" +
            "¿Hay ingredientes o alérgenos comunes que aparezcan con frecuencia en los platos?")
    
def opcion7():
    print("5.2.7 Comportamiento de los clientes \n" +
            "¿Existen pautas en las preferencias de los clientes en función de la época del año? \n" +
            "¿Los clientes con preferencias dietéticas específicas tienden a reservar en \n" +
            "restaurantes concretos?")
    
    
def opcion8():
    print("5.2.8 Garantía de calidad \n" +
            "¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas \n" +
            "reales realizadas?") 
    
def opcion9():
    print("5.2.9 Análisis de mercado \n" +
            "¿Cómo se comparan los precios de las habitaciones de los distintos hoteles y \n" +
            "existen valores atípicos?")
    
def opcion10():
    print("\n                                               *** Adios *** \n")
    
    
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
    

    
    
    
    
    


import consulta1

salir=False
while salir == False:
    informacion()
    opcion = int(input("\n Selecciona una opción: \n"))
    
    if opcion == 1:
        opcion1()
        
        consulta1.select()
        salir = input("\n Quiere seguir? \n")
        
    elif opcion == 2:
        opcion2()
    elif opcion == 3:
        opcion3()
    elif opcion == 4:
        opcion4()
    if opcion == 5:
        opcion5()
    elif opcion == 6:
        opcion6()
    elif opcion == 7:
        opcion7()
    elif opcion == 8:
        opcion8()
    elif opcion == 9:
        opcion9()
        
    elif opcion == 10:
        opcion10()
        salir=True
    else:
        print("Opción no válida")



















'''
5 LOAD: Data Warehouse
5.1 Data loading
Los datos transformados se cargarán en Postgres para su análisis posterior en 4
tablas distintas que responderán a las preguntas del Data analytics. Solo poner la
información de cada tabla que sea interesante para resolver estas preguntas.
5.2 Data analytics
Usando Apache Spark tenéis que obtener los datos a través de postgres y realizar
consultas que contengan análisis avanzados sobre los datos almacenados en el
almacén de datos.
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