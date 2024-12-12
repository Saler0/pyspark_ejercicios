# 1. Consider a file (wines.txt) containing the following data:
# wines.txt
# type 1,2.064254
# type 3,2.925376
# type 2,2.683955
# type 1,2.991452
# type 2,2.861996
# type 1,2.727688
# Provide the ordered list of Spark operations (no need to follow the exact syntax, just the kind of operation
# and main parameters) you’d need to retrieve the minimum value per type. Do not use SQL and minimize the
# use of other Python libraries or code. Save the results in output.txt.

from pyspark import SparkContext, SparkConf
import shutil
import os

# Crear la configuración y contexto de Spark
conf = SparkConf().setAppName("MinValueByType")
sc = SparkContext(conf=conf)

# Leer el archivo como RDD
data = sc.textFile("wines.txt")

# Transformar los datos
cleaned_data = data.map(lambda line: line.split(',')) \
                   .map(lambda x: (x[0].strip(), float(x[1].strip())))

# Encontrar el mínimo por tipo
min_values = cleaned_data.reduceByKey(lambda x, y: min(x, y))

# Verificar si el archivo ya existe y eliminarlo si es necesario
output_path = "output1.txt"
if os.path.exists(output_path):
    os.remove(output_path)  # Eliminar archivo si ya existe

# Guardar los resultados
with open(output_path, 'w') as f:
    for key, value in min_values.collect():
        f.write(f"{key},{value}\n")


# Detener el contexto de Spark
sc.stop()

