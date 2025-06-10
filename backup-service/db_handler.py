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

def save_backup_records(filename, size, structure):
    """
    Guarda los registros de un nuevo respaldo en la base de datos.
    Inserta en BackupInstances y BackedUpFiles.
    """
    conn = get_db_connection()
    if conn is None:
        raise Exception("No se pudo conectar a la base de datos para guardar el respaldo.")
    
    try:
        with conn.cursor() as cur:
            # Insertar la instancia principal del respaldo y obtener su ID
            cur.execute(
                "INSERT INTO BackupInstances (timestamp, total_size, user_defined_structure) VALUES (%s, %s, %s) RETURNING id",
                (datetime.now(), size, structure)
            )
            instance_id = cur.fetchone()[0]
            print(f"[DBHandler] Creada BackupInstance con ID: {instance_id}")

            # Aquí se deberían añadir los registros de cada archivo individual.
            # Por ahora, se simula con uno solo.
            cur.execute(
                "INSERT INTO BackedUpFiles (backup_instance_id, path_within_source, size, file_hash) VALUES (%s, %s, %s, %s)",
                (instance_id, filename, size, "dummy_hash_value_for_now") # El hash real se calcularía en el servicio
            )
            print(f"[DBHandler] Creado registro en BackedUpFiles para '{filename}'")
            
            # Confirmar la transacción
            conn.commit()
            return True
    except Exception as e:
        # Revertir la transacción si algo falla
        conn.rollback()
        print(f"[DBHandler] Error al guardar registros: {e}")
        raise e # Relanzar la excepción para que el servicio la maneje
    finally:
        if conn:
            conn.close()