import psycopg
import os
from datetime import datetime

PAGE_SIZE_AUTO_JOBS = 2 

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

def list_auto_backup_jobs(page_number=1):
    """
    Consulta la base de datos y devuelve una lista paginada de trabajos
    de respaldo automático.
    """
    conn = get_db_connection()
    if conn is None:
        return {"status": "ERROR", "message": "No se pudo conectar a la base de datos."}
    
    jobs_list = []
    # Calcula el OFFSET basado en el número de página y el tamaño de la página.
    offset = (page_number - 1) * PAGE_SIZE_AUTO_JOBS
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, job_name, source_path, destination_structure, frequency_hours, last_run_timestamp
                FROM AutoBackupJobs
                ORDER BY id ASC
                LIMIT %s OFFSET %s;
            """, (PAGE_SIZE_AUTO_JOBS, offset))
            results = cur.fetchall()
            
            for row in results:
                job_id, job_name, source_path, dest_structure, freq_hours, last_run = row
                jobs_list.append({
                    "id": job_id,
                    "job_name": job_name,
                    "source_path": source_path,
                    "destination_structure": dest_structure,
                    "frequency_hours": freq_hours,
                    "last_run_timestamp": last_run.isoformat() if last_run else None
                })
        return jobs_list 
    except Exception as e:
        print(f"[DBHandler] Error al listar trabajos automáticos paginados: {e}", flush=True)
        return {"status": "ERROR", "message": f"Error al consultar trabajos automáticos: {str(e)}"} 
    finally:
        if conn:
            conn.close()

def update_auto_job_timestamp(job_id):
    """
    Actualiza el last_run_timestamp de un trabajo de respaldo automático
    a la hora actual.
    """
    conn = get_db_connection()
    if conn is None:
        return False, "No se pudo conectar a la base de datos."

    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE AutoBackupJobs SET last_run_timestamp = %s WHERE id = %s",
                (datetime.now(), job_id)
            )
            conn.commit()
            if cur.rowcount == 0:
                return False, f"No se encontró ningún trabajo con ID {job_id} para actualizar."
            return True, f"Timestamp actualizado para el trabajo ID {job_id}."
    except Exception as e:
        print(f"[DBHandler] Error al actualizar timestamp del trabajo {job_id}: {e}", flush=True)
        conn.rollback()
        return False, f"Error al actualizar timestamp: {str(e)}"
    finally:
        if conn:
            conn.close()