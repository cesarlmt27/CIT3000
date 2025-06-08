import psycopg
import os

def get_db_connection():
    """Crea y retorna una nueva conexión a la base de datos."""
    try:
        conn_string = f"dbname='{os.getenv('DB_NAME')}' user='{os.getenv('DB_USER')}' host='{os.getenv('DB_HOST')}' password='{os.getenv('DB_PASS')}'"
        
        conn = psycopg.connect(conn_string)
        return conn
    except psycopg.OperationalError as e:
        print(f"[DBHandler] Error al conectar con la base de datos: {e}")
        return None

def list_backup_instances():
    """
    Consulta la base de datos y devuelve una lista de todas las
    instancias de respaldo.
    """
    conn = get_db_connection()
    if conn is None:
        return "Error: No se pudo conectar a la base de datos."
    
    try:
        # 'with' asegura que el cursor y la conexión se cierren
        with conn.cursor() as cur:
            # Ejecuta una consulta a la tabla que definiste en tu informe
            cur.execute("SELECT id, timestamp, total_size, user_defined_structure FROM BackupInstances ORDER BY timestamp DESC;")
            instances = cur.fetchall()
            
            # Formatear la salida para que sea legible
            if not instances:
                return "No hay respaldos registrados."
            
            # Crear una cadena de texto con los resultados
            response = "\n--- Lista de respaldos ---\n"
            for instance in instances:
                # id, fecha, tamaño, estructura
                response += f"ID: {instance[0]}, Fecha: {instance[1].strftime('%Y-%m-%d %H:%M:%S')}, Tamaño: {instance[2]} bytes, Estructura: {instance[3]}\n"
            
            return response

    except Exception as e:
        return f"Error al consultar la base de datos: {e}"
    finally:
        if conn:
            conn.close()