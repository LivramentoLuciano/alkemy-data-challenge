import os
import requests
from datetime import datetime
import pandas as pd

# Constantes
TODAY = datetime.today()
MONTH_DICT = {
    1: 'enero',
    2: 'febrero',
    3: 'marzo',
    4: 'abril',
    5: 'mayo',
    6: 'junio',
    7: 'julio',
    8: 'agosto',
    9: 'septiembre',
    10: 'octubre',
    11: 'noviembre',
    12: 'diciembre',    
}

## Carga de datos
MUSEUMS_URL = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv'
CINEMAS_URL = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv'
LIBRARIES_URL = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv'

# Categorias (datasets)
CATEGORY_MUSEUMS = 'museos'
CATEGORY_LIBRARIES = 'bibliotecas'
CATEGORY_CINEMAS = 'cines'

# Construye path a los csv
def build_file_path(category:str, date_loaded:datetime=TODAY) ->str:
    '''
    Dado una categoria, año y mes, construye la url al archivo local donde el mismo se encuentra guardado.
    Siguiente las especificaciones brindadas en la documentación.
    Por defecto, asume la fecha actual.

    path = 'data/{category}/{year}-{month}/{category}-date'

    >>> build_file_path('museos',(2022,5,25))
    data/museos/2022-5/museos-25-5-2022
    '''
    month_name = MONTH_DICT[date_loaded.month]
    date_locale = f'{date_loaded.day}-{date_loaded.month}-{date_loaded.year}'

    # debo crear el directorio si no existe
    folder = f'data/{category}/{date_loaded.year}-{month_name}'
    os.makedirs(folder, exist_ok=True) 

    filename = f'{category}-{date_locale}.csv'
    path = folder + '/' + filename
    return path

# Carga dataset de la web
def update_data(url: str, data_category: str) ->None:
    '''
    Actualiza el dataset indicado mediante la url. Lo descarga y guarda localmente.
    El nombre asignado sigue la nomenclatura detallada en la documentación del ejercicio
    y también puede observarse en la descripción del método build_file_path().
    
    Nota: el archivo creado se guarda con la fecha de carga del día en curso.

    >>> update_data('MUSEUMS_URL', 'museos')
    None (guarda archivo en "data/...")
    '''
    # contenido de la url (Response.content)
    try:
        req = requests.get(url)
        url_content = req.content

        # lo guardo localmente en archivo csv, segun fecha de carga y categoria 
        save_path = build_file_path(data_category, TODAY)
        file = open(save_path, 'wb')
        file.write(url_content)
        file.close()
    except Exception as e:
        print(f'Error actualizando el dataset de {data_category} desde la URL: {url}')

def update_data_museums():
    '''Actualiza la información del dataset Museos y la guarda de forma local en archivo csv.'''
    update_data(MUSEUMS_URL, CATEGORY_MUSEUMS)

def update_data_cinemas():
    '''Actualiza la información del dataset Cines y la guarda de forma local en archivo csv.'''
    update_data(CINEMAS_URL, CATEGORY_CINEMAS)

def update_data_libraries():
    '''Actualiza la información del dataset Librerias y la guarda de forma local en archivo csv.'''
    update_data(LIBRARIES_URL, CATEGORY_LIBRARIES)


# Carga datasets desde el archivo csv local
def load_data(category:str, dtype:dict=None, date_loaded:datetime=TODAY) -> pd.DataFrame:
    '''
    Devuelve un Dataframe a partir de los datos guardados localmente (csv), de la categoría indicada
    y la fecha estipulada (por defecto, el dia en curso).

    En caso de encontrar un error accediendo al archivo, devuelve un dataframe vacío.

    >>> load_data('museos')
    dataframe(museos_data_today)
    '''
    filepath = build_file_path(category)
    try:
        # dtype decidi resolverlo en el preprocessor
        data = pd.read_csv(filepath)
    except Exception as e:
        print(f'Hubo un error leyendo el archivo. Error: {e}')
        # devuelvo un dataframe vacio
        data = pd.DataFrame()

    return data

def load_data_museums() ->pd.DataFrame:
    '''
    Devuelve un dataframe con los datos ACTUALIZADOS (al dia en curso) del dataset Museos.

    >>> load_data_museums()
    dataframe(museos_data_today)
    '''
    # Primero, actualizo los datos al dia de la fecha (se guardan en csv)
    update_data_museums()
    # Luego, leo los datos desde el archivo actualizado
    data = load_data(CATEGORY_MUSEUMS)
    return data

def load_data_libraries() ->pd.DataFrame:
    '''
    Devuelve un dataframe con los datos ACTUALIZADOS (al dia en curso) del dataset Bibliotecas.

    >>> load_data_libraries()
    dataframe(libraries_data_today)
    '''    
    # Primero, actualizo los datos al dia de la fecha (se guardan en csv)
    update_data_libraries()
    # Luego, leo los datos desde el archivo actualizado
    data = load_data(CATEGORY_LIBRARIES)
    return data

def load_data_cinemas() ->pd.DataFrame:
    '''
    Devuelve un dataframe con los datos ACTUALIZADOS (al dia en curso) del dataset Cines.

    >>> load_data_cinemas()
    dataframe(cinemas_data_today)
    '''    
    # Primero, actualizo los datos al dia de la fecha (se guardan en csv)
    update_data_cinemas()
    # Luego, leo los datos desde el archivo actualizado
    data = load_data(CATEGORY_CINEMAS)
    return data

def load_full_data() -> tuple[pd.DataFrame]:
    museums = load_data_museums()
    cinemas = load_data_cinemas()
    libraries = load_data_libraries()
    return (museums, cinemas, libraries)
