# 2. Consider two files containing the following data:
# Employees.txt
# EMP1;CARME;400000;MATARO;DPT1
# EMP2;EUGENIA;350000;TOLEDO;DPT2
# EMP3;JOSEP;250000;SITGES;DPT3
# EMP4;RICARDO;250000;MADRID;DPT4
# EMP5;EULALIA;150000;BARCELONA;DPT5
# EMP6;MIQUEL;125000;BADALONA;DPT5
# EMP7;MARIA;175000;MADRID;DPT6
# EMP8;ESTEBAN;150000;MADRID;DPT6
# Departments.txt
# DPT1;DIRECCIO;10;PAU CLARIS;BARCELONA
# DPT2;DIRECCIO;8;RIOS ROSAS;MADRID
# DPT3;MARKETING;1;PAU CLARIS;BARCELONA
# DPT4;MARKETING;3;RIOS ROSAS;MADRID
# DPT5;VENDES;1;MUNTANER;BARCELONA
# DPT6;VENDES;1;CASTELLANA;MADRID
# Provide the ordered list of Spark operations (no need to follow the exact syntax, just the kind of operation
# and main parameters) you’d need to retrieve for each employee his/her department information. Do not use
# SQL and minimize the use of other Python libraries or code. Save the results in output.txt.

from pyspark import SparkContext, SparkConf
import shutil
import os

# Crear la configuración y contexto de Spark
conf = SparkConf().setAppName("DepartmenInforByEmp")
sc = SparkContext(conf=conf)

# Leer archivos como RDDs
departments = sc.textFile("Departments.txt")
employees = sc.textFile("Employees.txt")

# Transformar Departments.txt en (DPT, (DEPT_NAME, DEPT_NUM, ADDRESS, CITY))
departments_kv = departments.map(lambda line: line.split(";")) \
                            .map(lambda x: (x[0], (x[1], int(x[2]), x[3], x[4])))

# Transformar Employees.txt en (DPT, (EMP_ID, NAME, SALARY, CITY))
employees_kv = employees.map(lambda line: line.split(";")) \
                        .map(lambda x: (x[4], (x[0], x[1], int(x[2]), x[3])))
# Realizar el join
joined_rdds = employees_kv.join(departments_kv)

# Transformar los datos combinados al formato final
result_rdd = joined_rdds.map(lambda x: (x[1][0][0], x[1][0][1],x[0], x[1][1][0], x[1][1][1], x[1][1][2], x[1][1][3]))

# Verificar si el archivo ya existe y eliminarlo si es necesario
output_path = "output2.txt"
if os.path.exists(output_path):
    os.remove(output_path)  # Eliminar archivo si ya existe

# Guardar los resultados
with open(output_path, 'w') as f:
    for i in result_rdd.collect():
        f.write(f"{i}\n")


# Detener el contexto de Spark
sc.stop()