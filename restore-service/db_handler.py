# restore-service/db_handler.py
import psycopg
import os

def get_db_connection():
    try:
        conn_string = f"dbname='{os.getenv('DB_NAME')}' user='{os.getenv('DB_USER')}' host='{os.getenv('DB_HOST')}' password='{os.getenv('DB_PASS')}'"
        return psycopg.connect(conn_string)
    except psycopg.OperationalError as e:
        print(f"[RestoreDBHandler] Error al conectar: {e}", flush=True)
        return None

def get_backup_instance_details(instance_id):
    """Obtiene la estructura y el ID del trabajo automático de una instancia de respaldo."""
    conn = get_db_connection()
    if conn is None:
        return None, None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_defined_structure, auto_job_id FROM BackupInstances WHERE id = %s",
                (instance_id,)
            )
            result = cur.fetchone()
            if result:
                return result[0], result[1] # structure, auto_job_id
            return None, None
    except Exception as e:
        print(f"[RestoreDBHandler] Error al obtener detalles de instancia {instance_id}: {e}", flush=True)
        return None, None
    finally:
        if conn:
            conn.close()

def get_files_for_instance(instance_id, specific_files_relative_paths=None):
    """
    Obtiene la lista de archivos (ruta relativa, hash, tamaño) para una instancia de respaldo.
    Si specific_files_relative_paths se proporciona, filtra por esas rutas.
    """
    conn = get_db_connection()
    if conn is None:
        return []
    
    files_data = []
    try:
        with conn.cursor() as cur:
            query = """
                SELECT path_within_source, file_hash, size 
                FROM BackedUpFiles 
                WHERE backup_instance_id = %s
            """
            params = [instance_id]
            
            if specific_files_relative_paths:
                # Asegurarse de que specific_files_relative_paths sea una tupla para la consulta
                query += " AND path_within_source = ANY(%s)"
                params.append(list(specific_files_relative_paths))

            query += " ORDER BY path_within_source ASC;"
            
            cur.execute(query, tuple(params))
            results = cur.fetchall()
            for row in results:
                files_data.append({
                    "relative_path": row[0],
                    "hash": row[1],
                    "size": row[2]
                })
        return files_data
    except Exception as e:
        print(f"[RestoreDBHandler] Error al obtener archivos para instancia {instance_id}: {e}", flush=True)
        return []
    finally:
        if conn:
            conn.close()