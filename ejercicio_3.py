# 3. Consider an error log file (log.txt) like the one bellow:
# log.txt
# 20150323;0833;ERROR;Oracle
# 20150323;0835;WARNING;MySQL
# 20150323;0839;WARNING;MySQL
# 20150323;0900;WARNING;Oracle
# 20150323;0905;ERROR;MySQL
# 20150323;1013;OK;Oracle
# 20150323;1014;OK;MySQL
# 20150323;1055;ERROR;Oracle
# Provide the ordered list of Spark operations (no need to follow the exact syntax, just the kind of operation
# and main parameters) you’d need to retrieve the lines corresponding to both errors and warnings, but adding
# Important: at the beginning of the identifier of those of errors (i.e., only errors). Do not use SQL and
# minimize the use of other Python libraries or code. Save the results in output.txt.

from pyspark import SparkContext, SparkConf
import shutil
import os

# Crear la configuración y contexto de Spark
conf = SparkConf().setAppName("DepartmenInforByEmp")
sc = SparkContext(conf=conf)

# Leer archivos como RDDs
logs = sc.textFile("log.txt")

# Transformar logs.txt
logs_transform = logs.map(lambda line: line.split(";")) \
                    .filter(lambda x: x[2] == 'ERROR' or x[2] == 'WARNING')

# Verificar si el archivo ya existe y eliminarlo si es necesario
output_path = "output3.txt"
if os.path.exists(output_path):
    os.remove(output_path)  # Eliminar archivo si ya existe

# Guardar los resultados
with open(output_path, 'w') as f:
    for i in logs_transform.collect():
        if i[2] == 'ERROR':
            f.write(f"Important: {i}\n")
        else:
            f.write(f"{i}\n")


# Detener el contexto de Spark
sc.stop()