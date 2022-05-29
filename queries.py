### Consultas a la base de datos
### y generación de nuevas Tablas
import pandas as pd


# Campos de Tabla Conjunta (Museos + Cines + Bibliotecas)
FIELDS_COMBINED_DF = [
    'cod_localidad',
    'id_provincia',
    'id_departamento',
    'categoría',
    'provincia',
    'localidad',
    'nombre',
    'domicilio',
    'código_postal',
    'número_de_teléfono',
    'mail',
    'web',
    'fuente'
]

# Combino las tablas en 1 sola (asumo ya normalizadas)
def combine_dataframes(**kwargs) -> pd.DataFrame:
    '''
    Combina la información de Museos, Salas de cine y Bibliotecas populares en una sola tabla,
    conteniendo:
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
    
    Asumo que los dataframes recibidos ya fueron normalizados.

    >>> combine_dataframes(museums=museums_norm, cinemas=cinemas_norm, libraries=libraries_norm)

    '''
    # Dataframes a combinar
    df_museums = kwargs['museums'].copy()
    df_cinemas = kwargs['cinemas'].copy()
    df_libraries = kwargs['libraries'].copy()

    # retengo solo las columnas indicadas
    df_museums = df_museums[FIELDS_COMBINED_DF]
    df_cinemas = df_cinemas[FIELDS_COMBINED_DF]
    df_libraries = df_libraries[FIELDS_COMBINED_DF]    

    df_combined = pd.concat([df_museums, df_cinemas, df_libraries], axis=0)

    return df_combined

# Generación Tablas aggregate 
# (registros por categoria, por fuente, por provincia y categoria)
def get_categories_totals(cultural_df:pd.DataFrame) -> pd.DataFrame:
    '''
    A partir de la tabla de datos combinados (Museos, Salas de cine y Bibliotecas)
    generar una nueva tabla que presente la siguiente información:
        - Cantidad de registros totales por categoría
    '''
    df = cultural_df.copy()
    categories_totals = df.groupby(['categoría']).agg(total={'categoría': 'count'})
    categories_totals.sort_values(by=['total'], ascending=False, inplace=True)
    return categories_totals

def get_sources_totals(cultural_df:pd.DataFrame) -> pd.DataFrame:
    '''
    A partir de la tabla de datos combinados (Museos, Salas de cine y Bibliotecas)
    generar una nueva tabla que presente la siguiente información:
        - Cantidad de registros totales por fuente
    '''
    df = cultural_df.copy()
    sources_totals = df.groupby(['fuente']).agg(total={'fuente': 'count'})
    sources_totals.sort_values(by=['total'], ascending=False, inplace=True)
    return sources_totals    

def get_province_and_categorie_totals(cultural_df:pd.DataFrame) -> pd.DataFrame:
    '''
    A partir de la tabla de datos combinados (Museos, Salas de cine y Bibliotecas)
    generar una nueva tabla que presente la siguiente información:
        - Cantidad de registros por provincia y categoría    
    '''
    df = cultural_df.copy()
    
    # Agrupo por Provincia y Categoria y ordeno dentro de cada grupo
    prov_and_catg_totals = df.groupby(['provincia','categoría']).agg(total={'provincia': 'count'})
    prov_and_catg_totals.sort_values(by=['provincia','categoría','total'], ascending=False, inplace=True)
    prov_and_catg_totals.reset_index(inplace=True)

    return prov_and_catg_totals    


# Generación Tabla con data sobre salas de cine
def get_cinemas_summary(cinemas_df):
    '''
    Procesa los datos de los cines y genera una tabla con la siguiente información:
        - Provincia
        - Cantidad de pantallas
        - Cantidad de butacas
        - Cantidad de espacios INCAA    
    '''
    # dataframe normalizado con todos los datos sobre salas de cine en Argentina
    df = cinemas_df.copy()

    # el campo "espacio_incaa" contiene "Si"/"No", 
    # debo mapearlo a True/False o 1/0, para poder aggregate
    df['espacio_incaa'] = df['espacio_incaa'].apply(lambda x: x == "Si")

    res = df.groupby('provincia').agg({
        'pantallas':'sum',
        'butacas': 'sum',
        'espacio_incaa': 'sum'
    })

    return res
    