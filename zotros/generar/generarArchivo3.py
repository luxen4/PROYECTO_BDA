import random
import pandas as pd

# Leer el archivo CSV
df = pd.read_csv("ruta_del_archivo.csv")  # Reemplaza "ruta_del_archivo.csv" con la ruta real del archivo

# Generar listas de números aleatorios para la columna empleados
num_empleados = 5  # Número de empleados por fila
min_num = 1  # Valor mínimo de los números aleatorios
max_num = 100  # Valor máximo de los números aleatorios
df['empleados'] = [[random.randint(min_num, max_num) for _ in range(num_empleados)] for _ in range(len(df))]

# Mostrar el DataFrame resultante
print(df)


# Y ya después generar los nombres y demás atributos en un .csv .json o txt 