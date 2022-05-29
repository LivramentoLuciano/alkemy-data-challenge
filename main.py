from db import *
from utils import *
from preprocessor import *
from queries import *


# Cargo y guardo localmente los datos al dia de la fecha
museums_raw = load_data_museums()
cinemas_raw = load_data_cinemas()
libraries_raw = load_data_libraries()


# Preproceso los datos
print('Preproceso datasets...')
museums_prepr = preprocess_museums_data(museums_raw)
cinemas_prepr = preprocess_cinemas_data(cinemas_raw)
libraries_prepr = preprocess_libraries_data(libraries_raw)

# TODO: Normalizar las tablas (?)

# Cargo tablas normalizadas a base de datos
print('Cargando datos en postgres db...')
set_museums_table(museums_prepr)
set_cinemas_table(cinemas_prepr)
set_libraries_table(libraries_prepr)
print('Listo!')

print('Trayendo datos de postgres db...')
museums_df = get_museums_df()
# cinemas_df = get_cinemas_df()
# libraries_df = get_cinemas_df()
print('Listo!')
print(museums_df)
# print(cinemas_df)
# print(libraries_df)


# consultar base de datos y generar nuevas tablas
# cinemas_summary = get_cinemas_summary(cinemas_prepr)
# print(cinemas_summary)

# cargar nuevas tablas generadas a la base de datos


# fin
session.close()