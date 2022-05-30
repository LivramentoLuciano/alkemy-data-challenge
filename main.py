from db import *
from utils import *
from preprocessor import *

def main():
    # Cargo y guardo localmente los datos al dia de la fecha
    print('Actualizando datos de la fuente (gob.ar)...', end=' ')
    museums_raw = load_data_museums()
    cinemas_raw = load_data_cinemas()
    libraries_raw = load_data_libraries()
    print('Listo!')

    # Preproceso los datos
    print('Preproceso datasets...', end=' ')
    museums_prepr = preprocess_museums_data(museums_raw)
    cinemas_prepr = preprocess_cinemas_data(cinemas_raw)
    libraries_prepr = preprocess_libraries_data(libraries_raw)
    print('Listo!')

    # TODO: Normalizar las tablas (?)

    # Cargo tablas normalizadas a base de datos
    print('Cargando datos fuente normalizados en postgres db...', end=' ')
    set_museums_table(museums_prepr)
    set_cinemas_table(cinemas_prepr)
    set_libraries_table(libraries_prepr)
    print('Listo!')

    # Consultar tablas fuente y generar tabla conjunta
    print('Consulto db y genero tabla conjunta...', end=' ')
    combined_df = get_combined_df_from_sql()
    print('Listo!')

    # Cargar Tabla Conjunta a la base de datos
    print('Cargando tabla conjunta en postgres db...', end=' ')
    set_combined_table(combined_df)
    print('Listo!')

    # Consultar tabla conjunta y generar tablas de totales
    # Totales por categoria, fuente y provincia/categoria
    print('Consultando "totales por categoria", "totales por fuente" y "totales por provincia y categoria" en postgres db...', end=' ')
    categories_totals_df = get_categories_totals_from_sql()
    sources_totals_df = get_sources_totals_from_sql()
    province_and_category_totals_df = get_province_and_category_totals_from_sql()
    print('Listo!')

    # Cargar tablas generadas de totales a postgres db
    print('Cargando tablas de totales en postgres db...', end=' ')
    set_categories_totals_table(categories_totals_df)
    set_sources_totals_table(sources_totals_df)
    set_province_and_category_totals_table(province_and_category_totals_df)
    print('Listo!')

    # consultar tabla "salas de cine" y generar tablas "resumen salas de cine"
    print('Consultando "Resumen Salas de Cine" en postgres db...', end=' ')
    cinemas_summary_df = get_cinemas_summary_from_sql()
    print('Listo!')

    print('Cargando tabla "Resumen Salas de Cine" en postgres db...', end=' ')
    set_cinemas_summary_table(cinemas_summary_df)
    print('Listo!')

    # fin
    session.close()

if __name__ == '__main__':
    main()