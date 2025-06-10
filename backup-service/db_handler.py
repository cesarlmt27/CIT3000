# backup-service/db_handler.py
import psycopg
import os
from datetime import datetime

def get_db_connection():
    """Crea y retorna una nueva conexión a la base de datos."""
    try:
        conn_string = f"dbname='{os.getenv('DB_NAME')}' user='{os.getenv('DB_USER')}' host='{os.getenv('DB_HOST')}' password='{os.getenv('DB_PASS')}'"
        return psycopg.connect(conn_string)
    except psycopg.OperationalError as e:
        print(f"[DBHandler] Error al conectar: {e}")
        return None

def save_backup_records(structure, files_metadata):
    """
    Guarda los registros de un nuevo respaldo en la base de datos.

    Esta función es transaccional: inserta la instancia principal y luego
    todos los registros de archivos. Si algo falla, revierte todo.

    Args:
        structure (str): La estructura de directorios definida por el usuario.
        files_metadata (list): Una lista de diccionarios, donde cada uno
                               contiene 'relative_path', 'hash' y 'size'.
    """
    conn = get_db_connection()
    if conn is None:
        raise Exception("No se pudo conectar a la base de datos para guardar el respaldo.")
    
    # Se calcula el tamaño total sumando el tamaño de cada archivo.
    total_size = sum(f['size'] for f in files_metadata)

    try:
        # 'with' asegura que el cursor se cierre.
        with conn.cursor() as cur:
            # Insertar la instancia principal del respaldo y obtener su ID.
            cur.execute(
                "INSERT INTO BackupInstances (timestamp, total_size, user_defined_structure) VALUES (%s, %s, %s) RETURNING id",
                (datetime.now(), total_size, structure)
            )
            instance_id = cur.fetchone()[0]
            print(f"[DBHandler] Creada BackupInstance con ID: {instance_id}", flush=True)

            # Iterar sobre los metadatos para insertar cada archivo individualmente.
            for file_meta in files_metadata:
                print(f"[DBHandler] Insertando registro para: {file_meta['relative_path']}", flush=True)
                cur.execute(
                    "INSERT INTO BackedUpFiles (backup_instance_id, path_within_source, size, file_hash) VALUES (%s, %s, %s, %s)",
                    (
                        instance_id,
                        file_meta['relative_path'],
                        file_meta['size'],
                        file_meta['hash']
                    )
                )
            
            print(f"[DBHandler] Insertados {len(files_metadata)} registros en BackedUpFiles.", flush=True)
            
            # Confirmar todos los cambios en la base de datos, si no hay errores.
            conn.commit()
            return True
            
    except Exception as e:
        # Si ocurre cualquier error, revertir todos los cambios de esta transacción.
        print(f"[DBHandler] Error al guardar registros, revirtiendo transacción: {e}", flush=True)
        conn.rollback()
        raise e # Relanzar la excepción para que el servicio principal la maneje.
    finally:
        if conn:
            conn.close()