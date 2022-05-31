import streamlit as st
from db import *
from utils import *
from preprocessor import *

st.set_page_config(layout='wide')

# Cargo los datos de los archivos locales del dia de hoy (si no existen, empty)
@st.cache
def st_load_data():
    mus = load_data_museums()
    cin = load_data_cinemas()
    lib = load_data_libraries()
    return (mus, cin, lib)

def dataset_overview(df:pd.DataFrame):
    '''Mostrar Dataframe y valores característicos'''
    st.write('__Datos crudos__')
    st.dataframe(df)

    # st.write('###')
    # st.write('__Campos de datos__')
    # st.write(df.shape)
    # st.text(df.columns.values)
    # st.write('__Tipos de datos__')
    # st.text(df.dtypes)

def main():
    # cargo data
    museums, cinemas, libraries = st_load_data()

    st.header('Espacios culturales de la Argentina')

    # Datos Fuente
    with st.expander('Datos Fuente'):
        st.write('''
        Para este proyecto, se utilizan como fuente datasets de dominio público sobre: \
        __Museos__, __Salas de cine__ y __Bibliotecas Populares__ .
        Los mismos pueden encontrarse en los siguientes enlaces:
        - [Datos Argentina - Museos](https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_4207def0-2ff7-41d5-9095-d42ae8207a5d)
        - [Datos Argentina - Salas de cine](https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_392ce1a8-ef11-4776-b280-6f1c7fae16ae)
        - [Datos Argentina - Bibliotecas populares](https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_01c6c048-dbeb-44e0-8efa-6944f73715d7)

        ###
        __Inspección de los datos__

        A continuación, pueden visualizarse cada uno de estos en su forma original y una inspección de los mismos \
        a fin de observar qué tipo de información nos brindan y valores característicos.        
        ''')
        source_raw_option = st.selectbox('Seleccionar dataset:', ['Museos', 'Salas de cine', 'Bibliotecas'])

        if source_raw_option == 'Museos':
            dataset_overview(museums)
        elif source_raw_option == 'Salas de cine':
            dataset_overview(cinemas)
        else:
            dataset_overview(libraries)
           
    # Preprocesamiento
    with st.expander('Preprocesamiento'):
        st.write('''
        A partir de los datos fuente, se realiza un preprocesamiento de los mismos a fines de dar formato a estas tablas \
        y así poder trabajar con sus registros de manera eficiente y libre de errores indeseados.
        Incluye:
        - Conversión de dtype según indicado en la documentación técnica de la fuente de datos
        - Corrección de errores de input. Ejemplo: en dataset Cines, el campo 'espacio_INCAA' presenta valores: 'si', 'SI', 0
        - Manejo de Null, s/d, N/A
        - Unificación de nombres de los campos        
        ''')
        prepr_data_option = st.selectbox('Seleccionar dataset:', ['Museos', 'Salas de cine', 'Bibliotecas', 'i'])

        if prepr_data_option == 'Museos':
            museums_prep = preprocess_museums_data(museums)
            st.write('###')
            st.write('__Datos preprocesados__')
            st.write(museums_prep)   

        elif prepr_data_option == 'Salas de cine':
            cinemas_prep = preprocess_cinemas_data(cinemas)
            st.write('###')
            st.write('__Datos preprocesados__')
            st.write(cinemas_prep)

        else:
            libraries_prep = preprocess_libraries_data(libraries)
            st.write('###')
            st.write('__Datos preprocesados__')
            st.write(libraries_prep)                         

    # TODO: Normalizacion de tablas para base datos sql
    with st.expander('Normalización'):
        st.info('Proximamente...')

    # Consulto la db postgress y genero las tablas:
    # - Conjunta
    # - Total por categoria, Total por fuente y Total por provincia y categoria
    # - Resumen Salas de Cine
    with st.expander('Generación de nuevas tablas', expanded=True):
        st.write('''
        Finalmente, tras preprocesar las tablas de datos fuente, se realizan diferentes consultas a las mismas a fin de obtener\
        las distintas tablas informativas que se requiere: 
        - Tabla conjunta (Museos, Salas de Cine y Bilbiotecas)
        - Registros Totales, según:
            - Categoría
            - Fuente
            - Provincia y categoría
        - Resumen de datos sobre Salas de Cine (pantallas, butacas y "espacios_incaa" por provincia)
        ###
        ''')

        # Tabla Unificada
        st.write('__Tabla conjunta (Museos, Salas de cine y Bibliotecas)__')               
        combined_df = get_combined_df_from_sql()
        st.dataframe(combined_df)        

        # Totales por categoria, fuente y provincia/categoria
        st.write('###')
        st.write('__Grupos de registros__')           
        categories_totals = get_categories_totals_from_sql()
        sources_totals = get_sources_totals_from_sql()
        province_and_category_totals = get_province_and_category_totals_from_sql()

        option_groups = st.selectbox('Seleccione:', options=['Totales por Categoría', 'Totales por Fuente', 'Totales por Provincia y Categoría'])
        
        if option_groups == 'Totales por Categoría': 
            st.dataframe(categories_totals)
        elif option_groups == 'Totales por Fuente':
            st.dataframe(sources_totals)
        else:
            st.dataframe(province_and_category_totals)

        # Resumen Salas de cine
        cinemas_summary_df = get_cinemas_summary_from_sql()        
        st.write('###')
        st.write('__Resumen Salas de Cine__')
        st.dataframe(cinemas_summary_df)
    
    session.close()

if __name__ == '__main__':
    main()

