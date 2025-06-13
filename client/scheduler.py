import sys
import os
import time
import json
from datetime import datetime, timedelta
from bus_connector import transact

LOG_DIR = "/app/logs"
SCHEDULER_LOG_FILE = os.path.join(LOG_DIR, "scheduler.log")

def scheduler_loop(bus_host, bus_port):
    # Importar execute_backup aquí para evitar problemas de importación circular
    from handlers.backup_handler import execute_backup
    
    os.makedirs(LOG_DIR, exist_ok=True)
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    with open(SCHEDULER_LOG_FILE, 'a', buffering=1) as log_file_handle:
        sys.stdout = log_file_handle
        sys.stderr = log_file_handle

        print(f"\n--- [SchedulerScript] Log de respaldo automático iniciado a las {datetime.now()} ---", flush=True)
        
        admin_service_name = "admsv"

        try:
            while True:
                all_auto_jobs = []
                current_page = 1
                try:
                    print(f"\n[SchedulerScript] {datetime.now()}: Verificando trabajos de respaldo automático...", flush=True)
                    
                    while True:
                        page_payload_dict = {"page": current_page}
                        page_payload_json = json.dumps(page_payload_dict)
                        message_to_send = f"list_auto_jobs|{page_payload_json}"
                        
                        print(f"[SchedulerScript] Solicitando página {current_page} de trabajos automáticos...", flush=True)
                        r_service, r_status, r_content = transact(bus_host, bus_port, admin_service_name, message_to_send)

                        if r_status == "OK":
                            try:
                                jobs_on_page = json.loads(r_content)
                                if isinstance(jobs_on_page, dict) and jobs_on_page.get("status") == "ERROR":
                                    print(f"[SchedulerScript] Error del admin-service al obtener página {current_page}: {jobs_on_page.get('message')}", flush=True)
                                    all_auto_jobs = []
                                    break 
                                if not isinstance(jobs_on_page, list):
                                    print(f"[SchedulerScript] Error: Se esperaba una lista de trabajos en la página {current_page}, se recibió: {type(jobs_on_page)}. Contenido: {r_content}", flush=True)
                                    all_auto_jobs = [] 
                                    break 
                                if not jobs_on_page:
                                    print(f"[SchedulerScript] No más trabajos en la página {current_page}. Fin de la lista.", flush=True)
                                    break 
                                print(f"[SchedulerScript] Recibidos {len(jobs_on_page)} trabajo(s) en la página {current_page}.", flush=True)
                                all_auto_jobs.extend(jobs_on_page)
                                current_page += 1
                            except json.JSONDecodeError as e:
                                print(f"[SchedulerScript] Error al decodificar JSON de la página {current_page} de trabajos: {e}. Contenido: {r_content}", flush=True)
                                all_auto_jobs = [] 
                                break 
                        else:
                            print(f"[SchedulerScript] Error al obtener página {current_page} de trabajos automáticos del servicio '{r_service}': {r_content} (Estado: {r_status})", flush=True)
                            all_auto_jobs = [] 
                            break 
                    
                    if all_auto_jobs:
                        print(f"[SchedulerScript] {len(all_auto_jobs)} trabajo(s) automático(s) recibido(s) en total.", flush=True)
                        for job in all_auto_jobs:
                            job_id = job.get("id")
                            source_path = job.get("source_path")
                            dest_structure = job.get("destination_structure")
                            frequency_hours = job.get("frequency_hours")
                            last_run_str = job.get("last_run_timestamp")

                            if not all([job_id, source_path, dest_structure, frequency_hours is not None]):
                                print(f"[SchedulerScript] Trabajo incompleto recibido, omitiendo: {job}", flush=True)
                                continue

                            now = datetime.now()
                            should_run = False

                            if last_run_str:
                                try:
                                    last_run_time = datetime.fromisoformat(last_run_str)
                                    next_run_time = last_run_time + timedelta(hours=frequency_hours)
                                    if now >= next_run_time:
                                        should_run = True
                                except ValueError:
                                    print(f"[SchedulerScript] Error al parsear last_run_timestamp '{last_run_str}' para el trabajo {job_id}. Asumiendo que debe ejecutarse.", flush=True)
                                    should_run = True
                            else:
                                should_run = True
                            
                            if should_run:
                                print(f"[SchedulerScript] Ejecutando trabajo ID {job_id}: {source_path} -> {dest_structure}", flush=True)
                                effective_source_path = source_path 
                                if not os.path.isabs(source_path):
                                     effective_source_path = os.path.join("/app", source_path)
                                     print(f"[SchedulerScript] Ruta relativa detectada. Usando ruta efectiva: {effective_source_path}", flush=True)
                                
                                success = execute_backup(bus_host, bus_port, effective_source_path, dest_structure)
                                
                                if success:
                                    print(f"[SchedulerScript] Trabajo ID {job_id} completado exitosamente. Actualizando timestamp.", flush=True)
                                    update_payload = {"job_id": job_id}
                                    transact(bus_host, bus_port, admin_service_name, f"update_job_timestamp|{json.dumps(update_payload)}")
                                else:
                                    print(f"[SchedulerScript] Trabajo ID {job_id} falló.", flush=True)
                            elif last_run_str:
                                 next_run_time_display = (datetime.fromisoformat(last_run_str) + timedelta(hours=frequency_hours)).strftime('%Y-%m-%d %H:%M:%S')
                                 print(f"[SchedulerScript] Trabajo ID {job_id} no necesita ejecución. Próxima ejecución: {next_run_time_display}", flush=True)
                    elif current_page == 1 and not all_auto_jobs:
                         print(f"[SchedulerScript] No hay trabajos automáticos configurados o no se pudieron obtener.", flush=True)

                except ConnectionRefusedError:
                    print("[SchedulerScript] Error de conexión con el bus. Reintentando más tarde.", flush=True)
                except Exception as e:
                    print(f"[SchedulerScript] Error inesperado en el bucle del programador: {e}", flush=True)
                    import traceback
                    traceback.print_exc(file=sys.stderr) # Asegurar que el traceback vaya al log
                
                print(f"[SchedulerScript] Esperando 60 segundos para la próxima verificación...", flush=True)
                time.sleep(60)
        
        finally:
            print(f"--- [SchedulerScript] Log de respaldo automático finalizando a las {datetime.now()} ---", flush=True)
            sys.stdout = original_stdout
            sys.stderr = original_stderr

if __name__ == "__main__":
    # Obtener configuración del bus desde variables de entorno
    SCHEDULER_BUS_HOST = os.getenv("BUS_HOST", "localhost")
    SCHEDULER_BUS_PORT = int(os.getenv("BUS_PORT", 5000))
    
    print(f"[SchedulerScript Main] Iniciando scheduler.py como script independiente. BUS_HOST={SCHEDULER_BUS_HOST}, BUS_PORT={SCHEDULER_BUS_PORT}", flush=True)
    
    scheduler_loop(SCHEDULER_BUS_HOST, SCHEDULER_BUS_PORT)