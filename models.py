### Modelos de Tablas de mi Base de datos
###
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,  Integer, String, Float

# Clase Base para modelos de datos
Base = declarative_base()

# constantes
MUSEUMS_TABLE_NAME = 'Museos'
CINEMAS_TABLE_NAME = 'Salas de cine'
LIBRARIES_TABLE_NAME = 'Bibliotecas'

# Modelo de datos de Museos
class Museo(Base):
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


# dict de conversion, Nombre de tabla a Clase de datos
TABLE_NAME_CLASS_DICT = {
    MUSEUMS_TABLE_NAME: Museo,
    # CINEMAS_TABLE_NAME: Cinema,
    # LIBRARIES_TABLE_NAME: Library
    
}
