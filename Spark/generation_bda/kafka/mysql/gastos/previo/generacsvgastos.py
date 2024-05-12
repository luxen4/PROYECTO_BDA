# Supongamos que el formato es tipo_gasto,monto,fecha
import random
import csv, random
import datetime

def create_text_file(filename, content):
    with open(filename, 'w') as file:
        file.write(content)
    print(f"File '{filename}' created successfully!")


# Función para generar un nombre aleatorio
def generar_Concepto():
    nombres = ["Luz", "Agua", "Gas", "Limpieza", "Otros"]
    return random.choice(nombres)

def generafecha():
    fecha_inicial = datetime.date(2000, 1, 1)
    fecha_final = datetime.date(2024, 12, 31)

    diferencia_dias = (fecha_final - fecha_inicial).days
    fecha_aleatoria = fecha_inicial + datetime.timedelta(days=random.randint(0, diferencia_dias))
    fecha_aleatoria_str = fecha_aleatoria.strftime("%Y-%m-%d")

    #print("Fecha aleatoria generada:", fecha_aleatoria_str)
    return fecha_aleatoria_str


def generar_id_hotel():
    id_hotel = random.randint(1, 1000)
    return id_hotel


def generar_Monto():
    return random.randint(0, 10000)

def generar_Pagado():
    nombres = ["Si", "No"]
    return random.choice(nombres)


gastos=''
for _ in range(1000):
    gasto = "" + str(generafecha()) + "*" + str(generar_id_hotel()) + "*" +  str(generar_Concepto()) + "*" + str(generar_Monto()) + "*" + str(generar_Pagado()) + "\n"
    gastos=gastos+gasto


# fecha,id_hotel,concepto,monto,pagado
create_text_file("./Spark/data_bda/text/gastgastos.txt", gastos)