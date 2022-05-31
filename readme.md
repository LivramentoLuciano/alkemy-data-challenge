# Alkemy Data Analysis with Python Challenge 
Ejercicio técnico de evaluación para ingreso en el [Alkemy Lab - Data Analyst](https://campus.alkemy.org/inscription).

## Consigna
Crear un proyecto que consuma datos desde 3 fuentes distintas para popular una base de datos SQL con información cultural sobre bibliotecas, museos y salas de cines argentinos.

A partir de los datos fuente mencionados anteriormente, se deben generar las siguientes tablas:
- Tabla conjunta (Museos + Salas de cine + Bibliotecas)
- Cantidad de registros (a partir de la tabla conjunta) por:
    - Categoría
    - Fuente
    - Provincia y Categoría
- Resumen Salas de Cine (cantidad de pantallas, butacas y "espacios incaa", por provincia)

Para más detalles sobre las estructuras de datos y requerimientos técnicos a cumplir, dirigirse a la [documentación técnica del challenge](/data-challenge.pdf)

## Conocimientos requeridos
- Python (Pandas, SQLAlchemy, requests)
- SQL (PostgresSQL)

## Ejecución
1. Para ejecutar el programa localmente, primero se deberá clonar el repositorio:
```
git clone https://github.com/LivramentoLuciano/alkemy-data-challenge.git
```

2. Crear un entorno virtual
```
python -m venv venv
```

3. Instalar las dependencias requeridas (a partir del archivo "requirements.txt")
```
pip install -r requirements.txt
```

4. Ahora se debe configurar la conexión a la base de datos PostgreSQL. Para ello, crea un archivo `.env` en la raíz del proyecto (consejo: se puede obtener copiando el archivo `.env.example` dispuesto para facilitar esta tarea, tan sólo eliminar la extensión `.example`). En este archivo se dispondrá el string de conexión a la base de datos local, indicando tus credenciales de acceso, de la siguiente manera:
```
DB_URL=postgresql+psycopg2://tu_usuario:tu_password@localhost:5432/postgres
```

5. Ya puedes ejecutar el programa `main.py` que realizará la descarga, procesamiento y actualización de la base de datos.

## Extra: Visualización de datos con Streamlit
A fin de obtener una visualización más amigable de los datos, como así también de las distintas etapas del proyecto (obtención de datos fuente, preprocesamiento, generación de nuevas tablas), se realizó un pequeño script `app.py` que por medio de la librería `streamlit` nos permite desarrollar una web-app simple, la cual muestra todos los datos con una interfaz de usuario de gran calidad.

Para ejecutar este programa, simplemente ingresar el siguiente comando desde la raíz del proyecto:
```
streamlit run app.py
```

- Video ilustrativo de la app

https://user-images.githubusercontent.com/27143242/171085651-252d8598-ce20-4e9f-9299-eee7c771c062.mp4

## Todo's
- Normalización
- Logs
- Catch errores acceso a db
