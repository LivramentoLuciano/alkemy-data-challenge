### Preprocesamiento de los datos. 
### Incluye:
###     - Conversion de dtype segun indicado en la Web
###     - Corrección de errores de input
###       Ejemplo:
###                 - En dataset Cines, campo 'espacio_INCAA' presenta valores: 's', 'SI', 0
###                   deberia unificar criterio, para que 'si' valga lo mismo que 'SI'
###       TODO: Decidir manejo de Null, s/d, N/A
###     - Unificar nombres de los campos
import pandas as pd
from utils import TODAY

# Datatypes indicados en la Web (sino read_csv asume por default)
MUSEUMS_DTYPE = {
    'Cod_Loc': int,
    'IdProvincia': int,
    'IdDepartamento': int,
    'Observaciones': str,
    'categoria': str,
    'subcategoria': str,
    'provincia': str,
    'localidad': str,
    'nombre': str,
    'direccion': str,
    'piso': str,
    'CP': str,
    'cod_area': int,
    'telefono': int,
    'Mail': str,
    'Web': str,
    'Latitud': float,
    'Longitud': float,
    'TipoLatitudLongitud': str,
    'Info_adicional': str,
    'fuente': str,
    'jurisdiccion': str,
    'año_inauguracion': int,
    'actualizacion': int,
}

CINEMAS_DTYPE = {
    'Cod_Loc': int,
    'IdProvincia': int,
    'IdDepartamento': int,
    'Observaciones': str,
    'Categoría': str,
    'Provincia': str,
    'Departamento': str,
    'Localidad': str,
    'Nombre': str,
    'Dirección': str,
    'Piso': str,
    'CP': str,
    'cod_area': int,
    'Teléfono': int,
    'Mail': str,
    'Web': str,
    'Información adicional': str,
    'Latitud': float,
    'Longitud': float,
    'TipoLatitudLongitud': str,
    'Fuente': str,
    'tipo_gestion': str,
    'Pantallas': int,
    'Butacas': int,
    'espacio_INCAA': int,
    'año_actualizacion': int,
}

LIBRARIES_DTYPE = {
    'Cod_Loc': int,
    'IdProvincia': int,
    'IdDepartamento': int,
    'Observacion': str,
    'Categoría': str,
    'Subcategoria': str,
    'Provincia': str,
    'Departamento': str,
    'Localidad': str,
    'Nombre': str,
    'Domicilio': str,
    'Piso': str,
    'CP': str,
    'Cod_tel': int,
    'Teléfono': int,
    'Mail': str,
    'Web': str,
    'Información adicional': str,
    'Latitud': float,
    'Longitud': float,
    'TipoLatitudLongitud': str,
    'Fuente': str,
    'Tipo_gestion': str,
    'año_inicio': int,
    'Año_actualizacion': int,
}

def preprocess_museums_data(museums:pd.DataFrame) ->pd.DataFrame:
    '''
    Procesamiento de datos de Museos...
    Busca validar los datos del dataset, para pasar de los datos crudos 
    a datos con un formato uniforme que luego pueda aplicarle otros procesos
    de extraccion de información.

    >>> preprocess_museums_data(museums_raw)
    dataframe(museums_prepr)
    '''
    # dataframe a preprocesar
    museums_prepr = museums.copy()

    # Manejo valores s/d y NA
    museums_prepr = museums_prepr.apply(handle_col_sd)
    museums_prepr = museums_prepr.apply(handle_col_null)        

    # Convierto dtype segun indicado en la Web (en caso de conflicto, ignora la conversion)
    museums_prepr = museums_prepr.astype(MUSEUMS_DTYPE, errors='ignore')

    # Manejo errores de Input de sus diferentes campos (ejemplo: 'si' = 'SI')
    museums_prepr = handle_museums_input_error(museums_prepr)

    # Unificacion de nombres de los distintos campos
    museums_prepr = rename_data_fields(df=museums_prepr, norm_dict=MUSEUMS_FIELDS_NORM_DICT)

    # inserto campo 'id' y 'fecha_carga' para guardarlo en postgres db
    museums_prepr.insert(0,'id', museums_prepr.index)
    museums_prepr['fecha_carga'] = TODAY.date()

    return museums_prepr

def preprocess_cinemas_data(cinemas:pd.DataFrame) ->pd.DataFrame:
    '''
    Procesamiento de datos de Cines...
    Busca validar los datos del dataset, para pasar de los datos crudos 
    a datos con un formato uniforme que luego pueda aplicarle otros procesos
    de extraccion de información.

    >>> preprocess_cinemas_data(cinemas_raw)
    dataframe(cinemas_prepr)
    '''
    # dataframe a preprocesar
    cinemas_prepr = cinemas.copy()

    # Manejo valores "s/d" y NA
    cinemas_prepr = cinemas_prepr.apply(handle_col_sd)
    cinemas_prepr = cinemas_prepr.apply(handle_col_null)        

    # Convierto dtype segun indicado en la Web (en caso de conflicto, ignora la conversion)
    cinemas_prepr = cinemas_prepr.astype(CINEMAS_DTYPE, errors='ignore')

    # Manejo errores de Input de sus diferentes campos (ejemplo: 'si' = 'SI')
    cinemas_prepr = handle_cinemas_input_error(cinemas_prepr)

    # Unificacion de nombres de los distintos campos
    cinemas_prepr = rename_data_fields(df=cinemas_prepr, norm_dict=CINEMAS_FIELDS_NORM_DICT)

    # inserto campo 'id' y 'fecha_carga' para guardarlo en postgres db
    cinemas_prepr.insert(0,'id', cinemas_prepr.index)
    cinemas_prepr['fecha_carga'] = TODAY.date()

    # Devuelvo el dataframe cinemas_preprocesado, Ok
    return cinemas_prepr

def preprocess_libraries_data(libraries:pd.DataFrame) ->pd.DataFrame:
    '''
    Procesamiento de datos de Bibliotecas...
    Busca validar los datos del dataset, para pasar de los datos crudos 
    a datos con un formato uniforme que luego pueda aplicarle otros procesos
    de extraccion de información.

    >>> preprocess_libraries_data(libraries_raw)
    dataframe(libraries_prepr)
    '''
    # dataframe a preprocesar
    libraries_prepr = libraries.copy()

    # Manejo valores s/d y NA
    libraries_prepr = libraries_prepr.apply(handle_col_sd)
    libraries_prepr = libraries_prepr.apply(handle_col_null)

    # Convierto dtype segun indicado en la Web (en caso de conflicto, ignora la conversion)
    libraries_prepr = libraries_prepr.astype(LIBRARIES_DTYPE, errors='ignore')

    # Manejo errores de Input de sus diferentes campos (ejemplo: 'si' = 'SI')
    libraries_prepr = handle_libraries_input_error(libraries_prepr)

    # Unificacion de nombres de los distintos campos
    libraries_prepr = rename_data_fields(df=libraries_prepr, norm_dict=LIBRARIES_FIELDS_NORM_DICT)

    # inserto campo 'id' y 'fecha_carga' para guardarlo en postgres db
    libraries_prepr.insert(0,'id', libraries_prepr.index)
    libraries_prepr['fecha_carga'] = TODAY.date()
    
    return libraries_prepr


# Manejo de valores "s/d"
def handle_col_sd(col:pd.Series):
    '''Recibe una Series (columna de DataFrame) y convierte sus valores "s/d" a None'''
    col_form = col.copy()
    col_form = col_form.apply(lambda x: x if x not in ['s/d', 'S/D'] else None)
    return col_form

# Manejo de valores Null
def handle_col_null(col:pd.Series):
    '''Recibe una Series (columna de DataFrame) y... '''
    col_form = col.copy()
    # col_form = col_form.apply(lambda x: x if True else None)
    # fillna()
    return col_form


# Constantes para unificar nombres de los campos de datos
MUSEUMS_FIELDS_NORM_DICT = {
    # Los que se van a combinar en tabla completa
    'Cod_Loc': 'cod_localidad',
    'IdProvincia': 'id_provincia',
    'IdDepartamento': 'id_departamento',
    'categoria': 'categoría',
    'provincia': 'provincia',
    'localidad': 'localidad',
    'nombre': 'nombre',
    'direccion': 'domicilio',
    'CP': 'código_postal',
    'telefono': 'número_de_teléfono',
    'Mail': 'mail',
    'Web': 'web',
    'fuente': 'fuente',
    # el resto
    'Observaciones': 'observaciones',
    'subcategoria': 'subcategoría',
    'piso': 'piso',
    'cod_area': 'código_área',
    'Latitud': 'latitud',
    'Longitud': 'longitud',
    'TipoLatitudLongitud': 'tipo_latitud_longitud',
    'Info_adicional': 'info_adicional',
    'jurisdiccion': 'gestión',
    'año_inauguracion': 'año_inauguración',
    'actualizacion': 'año_actualización',
}

CINEMAS_FIELDS_NORM_DICT = {
    # los que se van a combinar en tabla conjunta
    'Cod_Loc': 'cod_localidad',
    'IdProvincia': 'id_provincia',
    'IdDepartamento': 'id_departamento',
    'Categoría': 'categoría',
    'Provincia': 'provincia',
    'Localidad': 'localidad',
    'Nombre': 'nombre',
    'Dirección': 'domicilio',
    'CP': 'código_postal',
    'Teléfono': 'número_de_teléfono',
    'Mail': 'mail',
    'Web': 'web',
    'Fuente': 'fuente',
    # el resto
    'Observaciones': 'observaciones',
    'Departamento': 'departamento',
    'Piso': 'piso',
    'cod_area': 'código_área',
    'Información adicional': 'info_adicional',
    'Latitud': 'latitud',
    'Longitud': 'longitud',
    'TipoLatitudLongitud': 'tipo_latitud_longitud',
    'tipo_gestion': 'gestión',
    'Pantallas': 'pantallas',
    'Butacas': 'butacas',
    'espacio_INCAA': 'espacio_incaa',
    'año_actualizacion': 'año_actualización',
}

LIBRARIES_FIELDS_NORM_DICT = {
    # Campos que se combinarán en Tabla conjunta
    'Cod_Loc': 'cod_localidad',
    'IdProvincia': 'id_provincia',
    'IdDepartamento': 'id_departamento',
    'Categoría': 'categoría',
    'Provincia': 'provincia',
    'Localidad': 'localidad',
    'Nombre': 'nombre',
    'Domicilio': 'domicilio',
    'CP': 'código_postal',
    'Teléfono': 'número_de_teléfono',
    'Mail': 'mail',
    'Web': 'web',
    'Fuente': 'fuente',
    # el resto
    'Observacion': 'observaciones',
    'Subcategoria': 'subcategoría',
    'Departamento': 'departamento',
    'Piso': 'piso',
    'Cod_tel': 'código_área',
    'Información adicional': 'info_adicional',
    'Latitud': 'latitud',
    'Longitud': 'longitud',
    'TipoLatitudLongitud': 'tipo_latitud_longitud',
    'Tipo_gestion': 'gestión',
    'año_inicio': 'año_inauguración',
    'Año_actualizacion': 'año_actualización',    
}

# Unifico nomenclatura de campos
def rename_data_fields(**kwargs) ->pd.DataFrame:
    '''
    Unifica nomenclatura de los campos de Museos, Cines y Bilbiotecas cumpliendo con:
        - cod_localidad
        - id_provincia
        - id_departamento
        - categoría
        - provincia
        - localidad
        - nombre
        - domicilio
        - código postal
        - número de teléfono
        - mail
        - web
    
    >>> rename_data_fields(museums, MUSEUMS_FIELDS_NORM_DICT)
    dataframe(museums_data_fields_renamed)
    '''
    # Dataframes a normalizar y dict de unificacion de nombres de campos
    df = kwargs['df'].copy()
    norm_dict = kwargs['norm_dict'].copy()

    # renombro columnas, para compatibilidad entre dataframes
    df.rename(columns=norm_dict, inplace=True)

    return df

def handle_cinemas_input_error(cinemas:pd.DataFrame) ->pd.DataFrame:
    '''
    Recibe el dataframe de Cines y devuelve el mismo, habiendo corregido 
    errores de inputación (registros con diferente nomenclatura pero que 
    refieren a un mismo valor, ejemplo: 'si', 'SI').

    Se asume que sobre el mismo ya se ha resuelto errores de valores "s/d", NA
    y el dtype es el correcto.

    >>> handle_cinemas_input_error(cinemas)
    dataframe(cinemas_sin_errores_input)
    '''
    # Dataframe sobre el cual corregir los errores de input
    df = cinemas.copy()

    # Columnas con errores de input a corregir
    # ['espacio_INCAA', 'Piso',]
    df['espacio_INCAA'] = df['espacio_INCAA'].apply(handle_cinemas_espacioincaa_error)
    df['Piso'] = df['Piso'].apply(handle_piso_error)

    # TODO: handle_provincias_error

    # Devuelvo el dataframe con los errores de input corregidos
    return df

def handle_museums_input_error(museums:pd.DataFrame) ->pd.DataFrame:
    '''
    Recibe el dataframe de Museos y devuelve el mismo, habiendo corregido 
    errores de inputación (registros con diferente nomenclatura pero que 
    refieren a un mismo valor, ejemplo: 'piso 1', 'Piso 1').

    Se asume que sobre el mismo ya se ha resuelto errores de valores "s/d", NA
    y el dtype es el correcto.

    >>> handle_museums_input_error(museums)
    dataframe(museums_sin_errores_input)
    '''
    # Dataframe sobre el cual corregir los errores de input
    df = museums.copy()

    # Columnas con errores de input a corregir
    # ['fuente', 'jurisdiccion', 'piso']
    df['fuente'] = df['fuente'].apply(handle_museums_fuente_error)
    df['jurisdiccion'] = df['jurisdiccion'].apply(handle_museums_jurisdiccion_error)
    df['piso'] = df['piso'].apply(handle_museums_piso_error)

    # Devuelvo el dataframe con los errores de input corregidos
    return df

def handle_libraries_input_error(libraries:pd.DataFrame) ->pd.DataFrame:
    '''
    Recibe el dataframe de Museos y devuelve el mismo, habiendo corregido 
    errores de inputación (registros con diferente nomenclatura pero que 
    refieren a un mismo valor, ejemplo: 'piso 1', 'Piso 1').

    Se asume que sobre el mismo ya se ha resuelto errores de valores "s/d", NA
    y el dtype es el correcto.

    >>> handle_libraries_input_error(libraries)
    dataframe(libraries_sin_errores_input)
    '''
    # Dataframe sobre el cual corregir los errores de input
    df = libraries.copy()

    # Columnas con errores de input a corregir
    # ['provincia', 'piso',]
    df['Provincia'] = df['Provincia'].apply(handle_libraries_provincia_error)
    df['Piso'] = df['Piso'].apply(handle_libraries_piso_error)

    # Devuelvo el dataframe con los errores de input corregidos
    return df

## Correcciones de errores de input en dataset Cines
##
# Corrige errores input 'espacio_INCAA'
def handle_cinemas_espacioincaa_error(x):
    '''Corrección de errores de input del campo "espacio_INCAA, del dataset Cines"'''
    if x in ['si', 'SI', 'Si', 1]: return 1
    elif x in ['no', 'NO', 'No', 0]: return 0
    else: return None

# TODO: Corrige errores input 'piso'
def handle_cinemas_piso_error(x):
    '''Corrección de errores de input del campo "piso", del dataset Cines"'''    
    return handle_piso_error(x)


## Correcciones de errores de input en dataset Museos
##
# Corrige errores input 'fuente', del dataset Museos
def handle_museums_fuente_error(x):
    '''Corrección de errores de input del campo "fuente", del dataset Museos"'''
    FUENTE_INPUT_DICT = {
        'Gobierno de la provincia': 'Gobierno de la Provincia',
        'RCC- Córdoba': 'RCC',
    }

    # Si la fuente esta entre las que 
    # se encontraron con errores de input, la corrijo
    if x in FUENTE_INPUT_DICT:
        x = FUENTE_INPUT_DICT[x]
    
    return x

# Corrige errores input 'fuente', del dataset Museos
# TODO: resolver con regex, ver Unicode tambien
def handle_museums_jurisdiccion_error(x):
    '''Corrección de errores de input del campo "jurisdiccion", del dataset Museos"'''
    JURISDICCION_INPUT_DICT = {
        'Privado': 'Privada',
        'Mixta: Municipal / Privada': 'Mixta: Privada y Municipal',
        'Mixta-Privada/Municipal': 'Mixta: Privada y Municipal',
        'Mixta: Privada/Municipal': 'Mixta: Privada y Municipal',
        'Mixta: Municipal/Privada': 'Mixta: Privada y Municipal',
        'Mixto Provincial y Privado': 'Mixta: Privado y Provincial',
        'Mixta Provincial/Municipal': 'Mixta: Provincial y Municipal',
        'Provincial y Municipal': 'Mixta: Provincial y Municipal',
        'Mixta, Nacional y Municipal': 'Mixta: Nacional y Municipal',
    }

    # Si la jurisdiccion esta entre las que 
    # se encontraron con errores de input, la corrijo
    if x in JURISDICCION_INPUT_DICT:
        x = JURISDICCION_INPUT_DICT[x]

    return x

# Corrige errores input 'piso' del dataset Museos
def handle_museums_piso_error(x):
    '''Corrección de errores de input del campo "piso", del dataset Museos"'''    
    return handle_piso_error(x)


## Correcciones de errores de input en dataset Bibliotecas
##
# Corrige errores input 'provincia', del dataset Bibliotecas
def handle_libraries_provincia_error(x):
    '''Corrección de errores de input del campo "provincia", del dataset Bibliotecas"'''
    PROVINCIA_INPUT_DICT = {
        'Santa Fe': 'Santa Fé',
    }

    # Si la provincia esta entre las que 
    # se encontraron con errores de input, la corrijo
    if x in PROVINCIA_INPUT_DICT:
        x = PROVINCIA_INPUT_DICT[x]

    return x

# Corrige errores input 'piso' del dataset Bibliotecas
def handle_libraries_piso_error(x):
    '''Corrección de errores de input del campo "piso", del dataset Bibliotecas"'''    
    return handle_piso_error(x)


# Corrige errores input 'piso' (base reutilizada)
def handle_piso_error(x):
    '''Corrección de errores de input del campo "piso" (base reutilizable)"'''
    PISO_INPUT_DICT = {
        'piso 1': 'Piso 1',
        'Planta Alta': 'Piso 1',        
    }

    # Si el "piso" esta entre los que 
    # se encontraron con errores de input, lo corrijo   
    if x in PISO_INPUT_DICT:
        x = PISO_INPUT_DICT[x] 

    return x    