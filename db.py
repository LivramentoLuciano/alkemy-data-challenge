### Accesos a la Base de Datos, mediante SQLAlchemy
from settings import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import *
from contextlib import contextmanager
import pandas as pd

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

def get_museums_df() ->pd.DataFrame:
    '''
    Devuelve un pandas dataframe con la información de Museos
    a partir de una consulta a la base de datos postgresql.

    >>> get_museums_df()
    dataframe(museums_data)
    '''
    return get_df_from_sql_table(MUSEUMS_TABLE_NAME)

def get_cinemas_df() ->pd.DataFrame:
    '''
    Devuelve un pandas dataframe con la información de Salas de cine
    a partir de una consulta a la base de datos postgresql.

    >>> get_cinemas_df()
    dataframe(cinemas_data)
    '''
    return get_df_from_sql_table(CINEMAS_TABLE_NAME)

def get_libraries_df() ->pd.DataFrame:
    '''
    Devuelve un pandas dataframe con la información de Bibliotecas
    a partir de una consulta a la base de datos postgresql.

    >>> get_libraries_df()
    dataframe(libraries_data)
    '''
    return get_df_from_sql_table(LIBRARIES_TABLE_NAME)

