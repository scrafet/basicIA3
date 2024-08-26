import os
import pyodbc
from dotenv import load_dotenv
import google.generativeai as genai

# Cargar variables de entorno desde .env si existen
load_dotenv()

# Configuración de la conexión a MSSQL
MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")


# Función para establecer la conexión con el servidor MSSQL
def connect_to_server():
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={MSSQL_SERVER};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD}"
    try:
        connection = pyodbc.connect(connection_string)
        print(f"Conexión exitosa al servidor '{MSSQL_SERVER}'.")
        return connection
    except pyodbc.Error as e:
        print(f"Error al conectar con el servidor: {e}")
        return None


# Establecer la conexión al servidor
server_connection = connect_to_server()
if server_connection is None:
    exit("No se pudo establecer la conexión al servidor. Por favor, verifica las credenciales y la configuración.")


# Listar las bases de datos disponibles en el servidor
def list_databases():
    cursor = server_connection.cursor()
    cursor.execute("SELECT name FROM sys.databases")
    databases = cursor.fetchall()
    return [db[0] for db in databases]


# Listar los esquemas y contar las tablas en cada esquema de una base de datos
def list_schemas_and_tables(database):
    cursor = server_connection.cursor()
    cursor.execute(f"USE {database};")
    cursor.execute("""
    SELECT s.name AS schema_name, COUNT(t.name) AS table_count
    FROM sys.schemas s
    LEFT JOIN sys.tables t ON t.schema_id = s.schema_id
    GROUP BY s.name
    """)
    schemas = cursor.fetchall()
    return [(schema[0], schema[1]) for schema in schemas]


# Función para elegir la base de datos y el esquema
def choose_database_and_schema():
    databases = list_databases()
    print("Bases de datos disponibles:")
    for idx, db in enumerate(databases, 1):
        print(f"{idx}. {db}")

    db_choice = int(input("Selecciona el número de la base de datos que deseas usar: ")) - 1
    selected_db = databases[db_choice]
    print(f"Has seleccionado la base de datos: {selected_db}")

    schemas = list_schemas_and_tables(selected_db)
    print(f"Schemas disponibles en {selected_db}:")
    for idx, (schema_name, table_count) in enumerate(schemas, 1):
        print(f"{idx}. {schema_name} ({table_count} tablas)")

    schema_choice = int(input("Selecciona el número del esquema que deseas usar: ")) - 1
    selected_schema = schemas[schema_choice][0]
    print(f"Has seleccionado el esquema: {selected_schema}")

    return selected_db, selected_schema


# Elige la base de datos y el esquema
selected_database, selected_schema = choose_database_and_schema()

# Establecer la conexión a la base de datos seleccionada
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={MSSQL_SERVER};DATABASE={selected_database};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD}"
connection = pyodbc.connect(connection_string)


# Función para seleccionar la versión de MSSQL
def select_mssql_version():
    versions = [
        "SQL Server 2008",
        "SQL Server 2012",
        "SQL Server 2014",
        "SQL Server 2016",
        "SQL Server 2017",
        "SQL Server 2019",
        "SQL Server 2022"
    ]
    print("Selecciona la versión de MSSQL:")
    for idx, version in enumerate(versions, 1):
        print(f"{idx}. {version}")

    version_choice = int(input("Selecciona el número de la versión de MSSQL que deseas usar: ")) - 1
    selected_version = versions[version_choice]
    print(f"Has seleccionado: {selected_version}")
    return selected_version


# Seleccionar la versión de MSSQL
selected_version = select_mssql_version()

# Configura la API de Gemini 1.5 Flash
API_KEY = os.getenv("API_KEY")
genai.configure(api_key=API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash', generation_config={"response_mime_type": "application/json"})


# Obtener el esquema de la base de datos seleccionada
def get_schema(selected_schema):
    cursor = connection.cursor()
    cursor.execute(f"""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{selected_schema}'
    """)
    schema = cursor.fetchall()
    return [{"table": row[0], "column": row[1], "type": row[2]} for row in schema]


# Función para ejecutar una consulta SQL
def query(sql_query: str):
    cursor = connection.cursor()
    cursor.execute(sql_query)
    columns = [column[0] for column in cursor.description]
    results = cursor.fetchall()
    return [dict(zip(columns, row)) for row in results]


# Función para enviar consultas a Gemini y recibir respuestas
def query_gemini(prompt):
    response = model.generate_content(prompt)
    return response.text


# Convertir consulta en lenguaje natural a SQL considerando la versión de MSSQL
def human_query_to_sql(human_query: str):
    # Obtenemos el esquema de la base de datos seleccionado
    database_schema = get_schema(selected_schema)

    # Preparamos el prompt para Gemini, incluyendo la versión de MSSQL
    system_message = f"""
    Given the following schema and the specified SQL Server version, write a SQL query that retrieves the requested information.
    Ensure that the SQL syntax is compatible with the specified version of Microsoft SQL Server.
    Return the SQL query inside a JSON structure with the key "sql_query".
    SQL Server Version: {selected_version}
    <schema>
    {database_schema}
    </schema>
    """
    user_message = human_query

    # Enviamos el esquema completo con la consulta al modelo
    prompt = f"{system_message}\nUser: {user_message}\n"
    ai_response = query_gemini(prompt)

    # Imprime la respuesta completa del modelo para depuración
    print("Respuesta del modelo:", ai_response)

    # Aquí asumimos que la respuesta es una consulta SQL válida en JSON
    try:
        result_dict = eval(ai_response)
        if "sql_query" in result_dict:
            return result_dict["sql_query"]
        else:
            return None
    except (SyntaxError, KeyError):
        return None


# Función principal para interactuar con el usuario
def main():
    print("Bienvenido al chat con IA (Escribe 'salir' para terminar)")
    while True:
        user_input = input("Tú: ")
        if user_input.lower() in ["salir", "exit", "quit"]:
            print("Terminando la sesión. ¡Hasta luego!")
            break

        # Convertir consulta en lenguaje natural a SQL
        sql_query = human_query_to_sql(user_input)
        if sql_query:
            print(f"SQL generada: {sql_query}")
            # Ejecutar la consulta en la base de datos MSSQL
            results = query(sql_query)
            print("Resultados de la base de datos:")
            for row in results:
                print(row)
        else:
            print("No se pudo generar una consulta SQL válida.")


if __name__ == "__main__":
    main()
