from utils import *
import pandas as pd

# Cargo y guardo localmente los datos al dia de la fecha
museums_raw = load_data_museums()
cinemas_raw = load_data_cinemas()
libraries_raw = load_data_libraries()

# normalizo datos (igualdad de nombres de columnas) y visualizo resultado
museums_norm, cinemas_norm, libraries_norm = normalize_source_data(
    museums=museums_raw, 
    cinemas=cinemas_raw, 
    libraries=libraries_raw
)

# print(museums_norm)
# print(cinemas_norm)
# print(libraries_norm)

# combino los dataframes normalizados
cultural_df = combine_dataframes(museums=museums_norm, cinemas=cinemas_norm, libraries=libraries_norm)
print(cultural_df)



