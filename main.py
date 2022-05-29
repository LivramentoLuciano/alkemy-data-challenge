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

# Normalizar las tablas (?)
#

# cargar tablas normalizada a base de datos


# consultar base de datos y generar nuevas tablas
cinemas_summary = get_cinemas_summary(cinemas_prepr)
print(cinemas_summary)

# cargar nuevas tablas generadas a la base de datos


# fin