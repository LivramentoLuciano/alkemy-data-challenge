### Modelos de Tablas de mi Base de datos
###
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,  Integer, String, Float

# Clase Base para modelos de datos
Base = declarative_base()

# constantes
MUSEUMS_TABLE_NAME = 'Museos'
CINEMAS_TABLE_NAME = 'Salas_de_cine'
LIBRARIES_TABLE_NAME = 'Bibliotecas'
COMBINED_TABLE_NAME = 'Sitios_culturales_unificado'
CINEMAS_SUMMARY_TABLE_NAME = 'Resumen_salas_de_cine'
CATEGORIES_TOTALS_TABLE_NAME = 'Totales_categoria'
SOURCES_TOTALS_TABLE_NAME = 'Totales_fuente'
PROVINCE_AND_CATEGORY_TOTALS_TABLE_NAME = 'Totales_provincia_y_categoria'

# Modelo de datos de Museos
class Museum(Base):
    __tablename__ = MUSEUMS_TABLE_NAME
    id = Column(Integer, primary_key=True, name='id')
    cod_localidad = Column(Integer, name='cod_localidad')
    id_provincia = Column(Integer, name='id_provincia')
    id_departamento = Column(Integer, name='id_departamento')
    categoria = Column(String, name='categoría')
    provincia = Column(String, name='provincia')
    localidad = Column(String, name='localidad')
    nombre = Column(String, name='nombre')
    domicilio = Column(String, name='domicilio')
    codigo_postal = Column(String, name='código_postal')
    numero_de_telefono = Column(Integer, name='número_de_teléfono')
    mail = Column(String, name='mail')
    web = Column(String, name='web')
    fuente = Column(String, name='fuente')
    observaciones = Column(String, name='observaciones')
    subcategoria = Column(String, name='subcategoría')
    piso = Column(String, name='piso')
    codigo_area = Column(Integer, name='código_área')
    latitud = Column(Float, name='latitud')
    longitud = Column(Float, name='longitud')
    tipo_latitud_longitud = Column(String, name='tipo_latitud_longitud')
    info_adicional = Column(String, name='info_adicional')
    gestion = Column(String, name='gestión')
    ano_inauguracion = Column(Integer, name='año_inauguración')
    ano_actualizacion = Column(Integer, name='año_actualización')

    # def __init__(self,)

    # Impresion del objeto al traerlo de la DB
    def __repr__(self):
        return f'Museo(id={self.id}, nombre="{self.nombre}, categoría:"{self.categoria}")'

# Modelo de datos de Cines
class Cinema(Base):
    __tablename__ = CINEMAS_TABLE_NAME
    id = Column(Integer, primary_key=True, name='id')
    cod_localidad = Column(Integer, name='cod_localidad')
    id_provincia = Column(Integer, name='id_provincia')
    id_departamento = Column(Integer, name='id_departamento')
    categoria = Column(String, name='categoría')
    provincia = Column(String, name='provincia')
    localidad = Column(String, name='localidad')
    nombre = Column(String, name='nombre')
    domicilio = Column(String, name='domicilio')
    codigo_postal = Column(String, name='código_postal')
    numero_de_telefono = Column(Integer, name='número_de_teléfono')
    mail = Column(String, name='mail')
    web = Column(String, name='web')
    fuente = Column(String, name='fuente')
    observaciones = Column(String, name='observaciones')
    departamento = Column(String, name='departamento')
    piso = Column(String, name='piso')
    codigo_area = Column(Integer, name='código_área')
    info_adicional = Column(String, name='info_adicional')
    latitud = Column(Float, name='latitud')
    longitud = Column(Float, name='longitud')
    tipo_latitud_longitud = Column(String, name='tipo_latitud_longitud')
    gestion = Column(String, name='gestión')
    pantallas = Column(Integer, name='pantallas')
    butacas = Column(Integer, name='butacas')
    espacio_incaa = Column(Integer, name='espacio_incaa')
    ano_actualizacion = Column(Integer, name='año_actualización')

    # Impresion del objeto al traerlo de la DB
    def __repr__(self):
        return f'Cine(id={self.id}, nombre="{self.nombre}, categoría:"{self.categoria}")'

# Modelo de datos de Bibliotecas
class Library(Base):
    __tablename__ = LIBRARIES_TABLE_NAME
    id = Column(Integer, primary_key=True, name='id')
    cod_localidad = Column(Integer, name='cod_localidad')
    id_provincia = Column(Integer, name='id_provincia')
    id_departamento = Column(Integer, name='id_departamento')
    categoria = Column(String, name='categoría')
    provincia = Column(String, name='provincia')
    localidad = Column(String, name='localidad')
    nombre = Column(String, name='nombre')
    domicilio = Column(String, name='domicilio')
    codigo_postal = Column(String, name='código_postal')
    numero_de_telefono = Column(Integer, name='número_de_teléfono')
    mail = Column(String, name='mail')
    web = Column(String, name='web')
    fuente = Column(String, name='fuente')
    observaciones = Column(String, name='observaciones')
    subcategoria = Column(String, name='subcategoría')
    departamento = Column(String, name='departamento')
    piso = Column(String, name='piso')
    codigo_area = Column(Integer, name='código_área')
    info_adicional = Column(String, name='info_adicional')
    latitud = Column(Float, name='latitud')
    longitud = Column(Float, name='longitud')
    tipo_latitud_longitud = Column(String, name='tipo_latitud_longitud')
    gestion = Column(String, name='gestión')
    ano_inauguracion = Column(Integer, name='año_inauguración')
    ano_actualizacion = Column(Integer, name='año_actualización')        

    # Impresion del objeto al traerlo de la DB
    def __repr__(self):
        return f'Librería(id={self.id}, nombre="{self.nombre}, categoría:"{self.categoria}")'

# Model de datos de CulturalSite (tabla conjunta)
class CulturalSite(Base):
    __tablename__= COMBINED_TABLE_NAME
    id = Column(Integer, primary_key=True, name='id')
    id_sitio = Column(Integer, name='id_sitio')
    cod_localidad = Column(String, name='cod_localidad')
    id_provincia = Column(String, name='id_provincia')
    id_departamento = Column(String, name='id_departamento')
    categoria = Column(String, name='categoría')
    provincia = Column(String, name='provincia')
    localidad = Column(String, name='localidad')
    nombre = Column(String, name='nombre')
    domicilio = Column(String, name='domicilio')
    codigo_postal = Column(String, name='código_postal')
    numero_de_telefono = Column(String, name='número_de_teléfono')
    mail = Column(String, name='mail')
    web = Column(String, name='web')
    fuente = Column(String, name='fuente')

    # Impresion del objeto al traerlo de la DB
    def __repr__(self):
        return f'SitioCultural(id={self.id}, nombre={self.nombre}, categoría={self.categoria})'


# dict de conversion, Nombre de tabla a Clase de datos
TABLE_NAME_CLASS_DICT = {
    MUSEUMS_TABLE_NAME: Museum,
    CINEMAS_TABLE_NAME: Cinema,
    LIBRARIES_TABLE_NAME: Library 
}

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
    'fuente',
]    

# Entidades de campos a utilizar para generar
# una tabla unica (Museos, Cines, Bibliotecas)
# TODO: resolver sin harcod
MUSEUM_ENTITIES_COMBINED_DF = (
    Museum.id.label('id_sitio'),
    Museum.cod_localidad.label('cod_localidad'),
    Museum.id_provincia.label('id_provincia'),
    Museum.id_departamento.label('id_departamento'),
    Museum.categoria.label('categoría'),
    Museum.provincia.label('provincia'),
    Museum.localidad.label('localidad'),
    Museum.nombre.label('nombre'),
    Museum.domicilio.label('domicilio'),
    Museum.codigo_postal.label('código_postal'),
    Museum.numero_de_telefono.label('número_de_teléfono'),
    Museum.mail.label('mail'),
    Museum.web.label('web'),
    Museum.fuente.label('fuente'),
)

CINEMA_ENTITIES_COMBINED_DF = (
    Cinema.id.label('id_sitio'),
    Cinema.cod_localidad.label('cod_localidad'),
    Cinema.id_provincia.label('id_provincia'),
    Cinema.id_departamento.label('id_departamento'),
    Cinema.categoria.label('categoría'),
    Cinema.provincia.label('provincia'),
    Cinema.localidad.label('localidad'),
    Cinema.nombre.label('nombre'),
    Cinema.domicilio.label('domicilio'),
    Cinema.codigo_postal.label('código_postal'),
    Cinema.numero_de_telefono.label('número_de_teléfono'),
    Cinema.mail.label('mail'),
    Cinema.web.label('web'),
    Cinema.fuente.label('fuente'),
)

LIBRARY_ENTITIES_COMBINED_DF = (
    Library.id.label('id_sitio'),
    Library.cod_localidad.label('cod_localidad'),
    Library.id_provincia.label('id_provincia'),
    Library.id_departamento.label('id_departamento'),
    Library.categoria.label('categoría'),
    Library.provincia.label('provincia'),
    Library.localidad.label('localidad'),
    Library.nombre.label('nombre'),
    Library.domicilio.label('domicilio'),
    Library.codigo_postal.label('código_postal'),
    Library.numero_de_telefono.label('número_de_teléfono'),
    Library.mail.label('mail'),
    Library.web.label('web'),
    Library.fuente.label('fuente'),
)



