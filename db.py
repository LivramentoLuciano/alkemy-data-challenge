### Accesos a la Base de Datos, mediante SQLAlchemy
from settings import *
from sqlalchemy import create_engine, union, func
from sqlalchemy.orm import sessionmaker, load_only
from models import *
from contextlib import contextmanager
import pandas as pd
from utils import TODAY

# conexión a la base de datos mediante Sqlalchemy
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

# Regeneracion de la DB y sus Tablas
def recreate_database():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

# accesos correctos a la base de datos, con contextmanager
@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()

# Pandas dataframe a Postgres Table
def set_table_from_df(df:pd.DataFrame, table_name:str) ->None:
    ''' 
    Setea la tabla indicada de la base de datos Postgres,
    a partir del dataframe pandas suministrado.
    '''
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    session.commit()
    return None

def set_museums_table(museums:pd.DataFrame) ->None:
    ''' 
    Setea la tabla Museos de la base de datos Postgres,
    a partir del dataframe pandas suministrado.
    '''
    df = museums.copy()
    return set_table_from_df(df, MUSEUMS_TABLE_NAME)

def set_cinemas_table(cinemas:pd.DataFrame) ->None:
    ''' 
    Setea la tabla Salas de Cine de la base de datos Postgres,
    a partir del dataframe pandas suministrado.
    '''
    df = cinemas.copy()
    return set_table_from_df(df, CINEMAS_TABLE_NAME)

def set_libraries_table(libraries:pd.DataFrame) ->None:
    ''' 
    Setea la tabla Bilbiotecas de la base de datos Postgres,
    a partir del dataframe pandas suministrado.
    '''
    df = libraries.copy()
    return set_table_from_df(df, LIBRARIES_TABLE_NAME)


# Postgres Table a Pandas dataframe
def get_df_from_sql_table(table_name:str) ->pd.DataFrame:
    '''
    Lee una tabla de Postgresql y devuelve sus valores en formato pandas dataframe.

    >>> get_df_from_sql_table(MUSEUMS_TABLE_NAME)
    dataframe(museums_data)
    '''
    # Clase de los objetos de la Tabla a consultar
    obj = TABLE_NAME_CLASS_DICT[table_name]

    # Fetch basico mediante ORM SQLAlchemy
    # Devuelve un array con los objetos, pero yo lo quiero en df...
    # data = session.query(obj).all()

    # ... entonces, utilizo pandas.read_sql_query
    # ORM equivalente a un SELECT
    query = session.query(obj)

    # A partir de la query anterior, puedo 
    df = pd.read_sql_query(
        sql = query.statement,
        con = query.session.bind
    )

    return df

def get_museums_df_from_sql() ->pd.DataFrame:
    '''
    Devuelve un pandas dataframe con la información de Museos
    a partir de una consulta a la base de datos postgresql.

    >>> get_museums_df()
    dataframe(museums_data)
    '''
    return get_df_from_sql_table(MUSEUMS_TABLE_NAME)

def get_cinemas_df_from_sql() ->pd.DataFrame:
    '''
    Devuelve un pandas dataframe con la información de Salas de cine
    a partir de una consulta a la base de datos postgresql.

    >>> get_cinemas_df()
    dataframe(cinemas_data)
    '''
    return get_df_from_sql_table(CINEMAS_TABLE_NAME)

def get_libraries_df_from_sql() ->pd.DataFrame:
    '''
    Devuelve un pandas dataframe con la información de Bibliotecas
    a partir de una consulta a la base de datos postgresql.

    >>> get_libraries_df()
    dataframe(libraries_data)
    '''
    return get_df_from_sql_table(LIBRARIES_TABLE_NAME)


# Obtener dataframe conjunto, a partir de Museos, Cines y Bibliotecas
def get_combined_df_from_sql() -> pd.DataFrame():
    '''
    Devuelve un pandas DataFrame conteniendo la información combinada
    de las distintas tablas base: Museos, Salas de Cine y Bibliotecas.

    Esta tabla unificada contiene los siguientes campos:
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
        // TODO: los siguientes
        - id_sitio
        - id 
    ''' 
    # consulto cada tabla base (solo las columnas deseadas)...
    ## load_only() no sirve, por un tema de como se ejecuta, 
    ## la funcion UNION vera todas las columnas
    # museums_query = session.query().options(load_only(*museum_entities))
    museums_query = session.query(*MUSEUM_ENTITIES_COMBINED_DF)    
    cinemas_query = session.query(*CINEMA_ENTITIES_COMBINED_DF)
    libraries_query = session.query(*LIBRARY_ENTITIES_COMBINED_DF)

    # ... y concateno las querys anteriores con UNION
    union_query = union(museums_query, cinemas_query, libraries_query)
    
    # Finalmente obtengo el dataframe resultante de ejecutar la consulta anterior
    df = pd.read_sql_query(sql=union_query, con=engine)

    # agrego campo 'id' y 'fecha_carga' para cargarlo en db postgres
    df.insert(0,'id', df.index)
    df['fecha_carga'] = TODAY.date()
    
    # return union_query
    return df

# Guardo Tabla conjunta en Base de datos postgresql
def set_combined_table(combined_df:pd.DataFrame) ->None:
    '''
    Guarda en la Base de Datos Postgresql, la Tabla Unificada
    (Museos + Bibliotecas + Salas de cine), a partir de un pandas dataframe
    con dicha información ya combinada.

    >>> set_combined_table(combined_df)
    None
    '''
    df = combined_df.copy()
    return set_table_from_df(df, COMBINED_TABLE_NAME)    


# Obtener DataFrame de registros por categoria, de la tabla conjunta
def get_categories_totals_from_sql() ->pd.DataFrame:
    '''
    A partir de la tabla de datos combinados (Museos, Salas de cine y Bibliotecas)
    generar una nueva tabla que presente la siguiente información:
        - Cantidad de registros totales por categoría
    '''
    # categories_totals = df.groupby(['categoría']).agg(total={'categoría': 'count'})
    # categories_totals.sort_values(by=['total'], ascending=False, inplace=True)

    categories_totals_query = session\
        .query(CulturalSite.categoria, func.count(CulturalSite.categoria).label('total'))\
        .group_by(CulturalSite.categoria)\
        .order_by(func.count(CulturalSite.categoria).desc())
    
    categories_totals_df = pd.read_sql_query(
        sql=categories_totals_query.statement, 
        con=categories_totals_query.session.bind
    )

    # agrego campo 'fecha_carga' para cargarlo en db postgres
    categories_totals_df['fecha_carga'] = TODAY.date()    

    return categories_totals_df

# Obtener DataFrame de registros por fuente, de la tabla conjunta
def get_sources_totals_from_sql() -> pd.DataFrame:
    '''
    A partir de la tabla de datos combinados (Museos, Salas de cine y Bibliotecas)
    generar una nueva tabla que presente la siguiente información:
        - Cantidad de registros totales por fuente
    '''

    sources_totals_query = session\
        .query(CulturalSite.fuente, func.count(CulturalSite.fuente).label('total'))\
        .group_by(CulturalSite.fuente)\
        .order_by(func.count(CulturalSite.fuente).desc())

    sources_totals_df = pd.read_sql_query(
        sql=sources_totals_query.statement, 
        con=sources_totals_query.session.bind
    )

    # agrego campo 'fecha_carga' para cargarlo en db postgres
    sources_totals_df['fecha_carga'] = TODAY.date()   

    return sources_totals_df

# Obtener DataFrame de registros por provincia y categoria, de la tabla conjunta
def get_province_and_category_totals_from_sql() -> pd.DataFrame:
    '''
    A partir de la tabla de datos combinados (Museos, Salas de cine y Bibliotecas)
    generar una nueva tabla que presente la siguiente información:
        - Cantidad de registros totales por provincia y categoría
    '''

    province_and_category_query = session\
        .query(CulturalSite.provincia, CulturalSite.categoria, func.count(CulturalSite.provincia).label('total'))\
        .group_by(CulturalSite.provincia, CulturalSite.categoria)\
        .order_by(CulturalSite.provincia.asc(), func.count(CulturalSite.provincia).desc())

    province_and_category_df = pd.read_sql_query(
        sql=province_and_category_query.statement, 
        con=province_and_category_query.session.bind
    )

    # agrego campo 'fecha_carga' para cargarlo en db postgres
    province_and_category_df['fecha_carga'] = TODAY.date()       

    return province_and_category_df

# Guarda Tabla "totales_categoria" en Base de datos posgresql
def set_categories_totals_table(categories_totals_df:pd.DataFrame) ->None:
    '''
    Guarda en la Base de Datos Postgresql, la Tabla "totales_categoria",
    a partir de un pandas dataframe con dicha información.

    >>> set_categories_totals_table(categories_totals_df)
    None
    '''
    df = categories_totals_df.copy()
    return set_table_from_df(df, CATEGORIES_TOTALS_TABLE_NAME)    

# Guarda Tabla "totales_fuente" en Base de datos posgresql
def set_sources_totals_table(sources_totals_df:pd.DataFrame) ->None:
    '''
    Guarda en la Base de Datos Postgresql, la Tabla "totales_fuente",
    a partir de un pandas dataframe con dicha información.

    >>> set_sources_totals_table(sources_totals_df)
    None
    '''
    df = sources_totals_df.copy()
    return set_table_from_df(df, SOURCES_TOTALS_TABLE_NAME)    

# Guarda Tabla "totales_provincia_y_categoria" en Base de datos posgresql
def set_province_and_category_totals_table(province_and_category_totals_df:pd.DataFrame) ->None:
    '''
    Guarda en la Base de Datos Postgresql, la Tabla "totales_provincia_y_categoria",
    a partir de un pandas dataframe con dicha información.

    >>> set_province_and_category_totals_table(province_and_category_totals_df)
    None
    '''
    df = province_and_category_totals_df.copy()
    return set_table_from_df(df, PROVINCE_AND_CATEGORY_TOTALS_TABLE_NAME)    


# Obtener DataFrame de "Resumen Salas de Cine"
def get_cinemas_summary_from_sql() ->pd.DataFrame:
    '''
    A partir de la tabla de Salas de Cine de la db postgres
    generar una nueva tabla que presente la siguiente información:
        - Provincia
        - Cantidad de pantallas
        - Cantidad de butacas
        - Cantidad de espacios INCAA    
    '''

    cinemas_summary_query = session\
        .query(
            Cinema.provincia, 
            func.sum(Cinema.pantallas).label('total_pantallas'),
            func.sum(Cinema.butacas).label('total_butacas'),
            func.sum(Cinema.espacio_incaa).label('total_espacio_incaa')
        )\
        .group_by(Cinema.provincia)\
        .order_by(Cinema.provincia.asc())

    cinemas_summary_df = pd.read_sql_query(
        sql=cinemas_summary_query.statement, 
        con=cinemas_summary_query.session.bind,
    )

    # agrego campo 'fecha_carga' para cargarlo en db postgres
    cinemas_summary_df['fecha_carga'] = TODAY.date()   

    return cinemas_summary_df    

# Guarda Tabla "Resumen Salas de Cine" en Base de datos posgresql
def set_cinemas_summary_table(cinemas_summary_df:pd.DataFrame) ->None:
    '''
    Guarda en la Base de Datos Postgresql, la Tabla "Resumen Salas de Cine",
    a partir de un pandas dataframe con dicha información.

    >>> set_cinemas_summary_table(cinemas_summary_df)
    None
    '''
    df = cinemas_summary_df.copy()
    return set_table_from_df(df, CINEMAS_SUMMARY_TABLE_NAME)    
