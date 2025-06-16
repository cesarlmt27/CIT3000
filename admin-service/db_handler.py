import psycopg
import os
from datetime import datetime

PAGE_SIZE_AUTO_JOBS = 2  # Número de trabajos automáticos por página
PAGE_SIZE_BACKUP_INSTANCES = 1 # Número de instancias por página

def get_db_connection():
    """Crea y retorna una nueva conexión a la base de datos."""
    try:
        conn_string = f"dbname='{os.getenv('DB_NAME')}' user='{os.getenv('DB_USER')}' host='{os.getenv('DB_HOST')}' password='{os.getenv('DB_PASS')}'"
        
        conn = psycopg.connect(conn_string)
        return conn
    except psycopg.OperationalError as e:
        print(f"[DBHandler] Error al conectar con la base de datos: {e}")
        return None

def list_backup_instances(page_number=1):
    """
    Consulta la base de datos y devuelve una lista paginada detallada de
    instancias de respaldo, sus archivos asociados y el trabajo automático de origen si aplica.
    """
    conn = get_db_connection()
    if conn is None:
        return "Error: No se pudo conectar a la base de datos."

    # Obtener los IDs de las instancias para la página actual
    instance_ids_for_page = []
    offset = (page_number - 1) * PAGE_SIZE_BACKUP_INSTANCES
    try:
        with conn.cursor() as cur_instances:
            cur_instances.execute("""
                SELECT id FROM BackupInstances
                ORDER BY timestamp DESC, id DESC
                LIMIT %s OFFSET %s;
            """, (PAGE_SIZE_BACKUP_INSTANCES, offset))
            results_instances = cur_instances.fetchall()
            if not results_instances:
                return "No hay más respaldos registrados para esta página." if page_number > 1 else "No hay respaldos registrados."
            instance_ids_for_page = [row[0] for row in results_instances]

        # Obtener todos los detalles para estas instancias específicas
        if not instance_ids_for_page: # Doble chequeo, aunque el anterior debería cubrirlo
             return "No hay respaldos registrados para esta página." if page_number > 1 else "No hay respaldos registrados."

        with conn.cursor() as cur_details:
            cur_details.execute("""
                SELECT
                    bi.id AS instance_id,
                    bi.timestamp AS instance_timestamp,
                    bi.user_defined_structure AS instance_structure,
                    bf.path_within_source AS file_path,
                    bf.size AS file_size,
                    bf.file_hash AS file_hash,
                    aj.job_name AS auto_job_name
                FROM
                    BackupInstances bi
                LEFT JOIN
                    BackedUpFiles bf ON bi.id = bf.backup_instance_id
                LEFT JOIN
                    AutoBackupJobs aj ON bi.auto_job_id = aj.id
                WHERE
                    bi.id = ANY(%s) -- Usar ANY para pasar la lista de IDs
                ORDER BY
                    bi.timestamp DESC, bi.id DESC, bf.path_within_source ASC;
            """, (instance_ids_for_page,)) # Pasar como una tupla conteniendo la lista
            results = cur_details.fetchall()

            if not results: # Debería haber resultados si instance_ids_for_page no estaba vacío
                return "No se encontraron detalles para los respaldos de esta página."

            processed_instances = {}
            for row in results:
                instance_id, timestamp, structure, file_path, file_size, file_hash, auto_job_name = row
                if instance_id not in processed_instances:
                    instance_details_str = f"ID de respaldo: {instance_id}\n"
                    instance_details_str += f"  Fecha: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    instance_details_str += f"  Estructura: {structure}\n"
                    if auto_job_name:
                        instance_details_str += f"  Origen: Trabajo automático ('{auto_job_name}')\n"
                    else:
                        instance_details_str += f"  Origen: Manual\n"
                    instance_details_str += f"  Archivos:\n"
                    processed_instances[instance_id] = {"details": instance_details_str, "files_str": "", "has_files": False, "timestamp": timestamp}

                if file_path:
                    processed_instances[instance_id]["files_str"] += f"    - Ruta: {file_path}, Tamaño: {file_size} bytes, Hash: {file_hash}\n"
                    processed_instances[instance_id]["has_files"] = True
            
            response_parts = []
            # Ordenar por timestamp y luego id para la salida final, usando los IDs obtenidos para la página
            
            final_response_str = "\n--- Lista detallada de respaldos (Página " + str(page_number) + ") ---\n"
            for inst_id in instance_ids_for_page: # Iterar en el orden de la página
                if inst_id in processed_instances:
                    data = processed_instances[inst_id]
                    final_response_str += data["details"]
                    if data["has_files"]:
                        final_response_str += data["files_str"]
                    else:
                        final_response_str += "    (Esta instancia de respaldo no contiene archivos)\n"
                    final_response_str += "\n" 
            
            # Indicar si podría haber más páginas
            if len(instance_ids_for_page) == PAGE_SIZE_BACKUP_INSTANCES:
                final_response_str += "--- Puede haber más páginas de resultados. ---\n"

            return final_response_str.strip()

    except Exception as e:
        # Loggear el error real en el servidor
        print(f"[DBHandler] Error al listar instancias de respaldo paginadas: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return f"Error al consultar la base de datos: {str(e)}"
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

def add_auto_backup_job(job_name, source_path, destination_structure, frequency_hours):
    """
    Inserta un nuevo trabajo de respaldo automático en la base de datos.
    """
    conn = get_db_connection()
    if conn is None:
        return False, "No se pudo conectar a la base de datos."

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO AutoBackupJobs (job_name, source_path, destination_structure, frequency_hours)
                VALUES (%s, %s, %s, %s) RETURNING id;
                """,
                (job_name, source_path, destination_structure, frequency_hours)
            )
            job_id = cur.fetchone()[0]
            conn.commit()
            return True, f"Trabajo de respaldo automático '{job_name}' creado exitosamente."
    except Exception as e:
        print(f"[DBHandler] Error al añadir trabajo automático: {e}", flush=True)
        conn.rollback()
        return False, f"Error al guardar el trabajo automático en la base de datos: {str(e)}"
    finally:
        if conn:
            conn.close()