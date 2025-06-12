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
    Consulta la base de datos y devuelve una lista detallada de todas las
    instancias de respaldo y sus archivos asociados.
    """
    conn = get_db_connection()
    if conn is None:
        return "Error: No se pudo conectar a la base de datos."
    
    try:
        with conn.cursor() as cur:
            # Consulta para obtener instancias de respaldo y sus archivos asociados
            cur.execute("""
                SELECT
                    bi.id AS instance_id,
                    bi.timestamp AS instance_timestamp,
                    bi.user_defined_structure AS instance_structure,
                    bf.path_within_source AS file_path,
                    bf.size AS file_size,
                    bf.file_hash AS file_hash
                FROM
                    BackupInstances bi
                LEFT JOIN
                    BackedUpFiles bf ON bi.id = bf.backup_instance_id
                ORDER BY
                    bi.timestamp DESC, bi.id DESC, bf.path_within_source ASC;
            """)
            results = cur.fetchall()
            
            if not results:
                return "No hay respaldos registrados."
            
            response = "\n--- Lista detallada de respaldos ---\n"
            current_instance_id = None
            
            for row in results:
                instance_id, timestamp, structure, file_path, file_size, file_hash = row
                
                if instance_id != current_instance_id:
                    if current_instance_id is not None:
                        response += "\n" # Añadir un salto de línea entre instancias
                    response += f"ID de Respaldo: {instance_id}\n"
                    response += f"  Fecha: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    response += f"  Estructura: {structure}\n"
                    response += f"  Archivos:\n"
                    current_instance_id = instance_id
                
                if file_path: # Si hay archivos asociados (LEFT JOIN puede dar NULLs si no hay archivos)
                    response += f"    - Ruta: {file_path}, Tamaño: {file_size} bytes, Hash: {file_hash}\n"
                elif current_instance_id == instance_id and not file_path: # Instancia sin archivos
                    response += f"    (Esta instancia de respaldo no contiene archivos)\n"

            return response

    except Exception as e:
        return f"Error al consultar la base de datos: {e}"
    finally:
        if conn:
            conn.close()