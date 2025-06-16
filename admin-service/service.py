# admin-service/service.py
import os
import time
import json
from bus_connector import ServiceConnector, transact
from db_handler import list_backup_instances, list_auto_backup_jobs, update_auto_job_timestamp, add_auto_backup_job, get_instance_files_for_deletion, delete_backup_instance_metadata

# --- Configuración del servicio ---
BUS_HOST = os.getenv("BUS_HOST")
BUS_PORT = 5000
SERVICE_NAME = os.getenv("SERVICE_NAME")

def process_request(data_received):
    """
    Contiene la lógica de negocio principal del servicio.
    """
    print(f"[ServiceLogic] Procesando solicitud: '{data_received}'", flush=True)
    
    command_parts = data_received.split('|', 1)
    command = command_parts[0]
    payload_str = command_parts[1] if len(command_parts) > 1 else None

    if command == "listar":
        page_number = 1
        if payload_str:
            try:
                payload = json.loads(payload_str)
                page_number = int(payload.get("page", 1))
                if page_number < 1: page_number = 1
            except (json.JSONDecodeError, ValueError, TypeError) as e:
                print(f"[ServiceLogic] Payload inválido para listar: '{payload_str}'. Usando página 1. Error: {e}", flush=True)
                page_number = 1
        else:
             print("[ServiceLogic] No se recibió payload para listar. Usando página 1.", flush=True)
        
        return list_backup_instances(page_number=page_number)
    
    elif command == "list_auto_jobs":
        page_number = 1 # Por defecto a la primera página
        if payload_str:
            try:
                payload = json.loads(payload_str)
                page_number = int(payload.get("page", 1))
                if page_number < 1: # Asegurar que el número de página sea positivo
                    page_number = 1
            except (json.JSONDecodeError, ValueError, TypeError) as e:
                print(f"[ServiceLogic] Payload inválido o faltante para list_auto_jobs: '{payload_str}'. Error: {e}. Usando página 1.", flush=True)
                page_number = 1
        else:
            # Si no hay payload, se asume la primera página.
            print("[ServiceLogic] No se recibió payload para list_auto_jobs. Usando página 1.", flush=True)
        
        jobs_page_data = list_auto_backup_jobs(page_number=page_number)
        
        if isinstance(jobs_page_data, dict) and jobs_page_data.get("status") == "ERROR":
            return json.dumps(jobs_page_data) # db_handler devolvió un error
        
        return json.dumps(jobs_page_data) # Devuelve la página actual de trabajos como JSON

    elif command == "update_job_timestamp":
        if not payload_str:
            return json.dumps({"status": "ERROR", "message": "Payload faltante para update_job_timestamp."})
        try:
            payload = json.loads(payload_str)
            job_id = payload.get("job_id")
            if job_id is None:
                return json.dumps({"status": "ERROR", "message": "job_id faltante en el payload."})
            
            success, message = update_auto_job_timestamp(job_id)
            if success:
                return json.dumps({"status": "OK", "message": message})
            else:
                return json.dumps({"status": "ERROR", "message": message})
        except json.JSONDecodeError:
            return json.dumps({"status": "ERROR", "message": "Payload JSON malformado."})
        except Exception as e:
            return json.dumps({"status": "ERROR", "message": f"Error interno: {str(e)}"})
            
    elif command == "add_auto_job":
        if not payload_str:
            return json.dumps({"status": "ERROR", "message": "Payload faltante para add_auto_job."})
        try:
            payload = json.loads(payload_str)
            job_name = payload.get("job_name")
            source_path = payload.get("source_path")
            destination_structure = payload.get("destination_structure")
            frequency_hours = payload.get("frequency_hours")

            if not all([job_name, source_path, destination_structure, frequency_hours is not None]):
                return json.dumps({"status": "ERROR", "message": "Faltan campos obligatorios en el payload (job_name, source_path, destination_structure, frequency_hours)."})

            if not isinstance(frequency_hours, int) or frequency_hours <= 0:
                return json.dumps({"status": "ERROR", "message": "frequency_hours debe ser un entero positivo."})

            success, message = add_auto_backup_job(job_name, source_path, destination_structure, frequency_hours)
            if success:
                return json.dumps({"status": "OK", "message": message})
            else:
                return json.dumps({"status": "ERROR", "message": message})
        except json.JSONDecodeError:
            return json.dumps({"status": "ERROR", "message": "Payload JSON malformado para add_auto_job."})
        except Exception as e:
            return json.dumps({"status": "ERROR", "message": f"Error interno al procesar add_auto_job: {str(e)}"})
            
    elif command == "delete_backup":
        if not payload_str:
            return json.dumps({"status": "ERROR", "message": "Payload faltante para delete_backup."})
        try:
            payload = json.loads(payload_str)
            instance_id = payload.get("instance_id")

            if instance_id is None:
                return json.dumps({"status": "ERROR", "message": "instance_id es requerido en el payload."})

            print(f"[ServiceLogic] Iniciando proceso de eliminación para instancia ID: {instance_id}", flush=True)

            # Obtener información de archivos de la instancia
            instance_structure, relative_file_paths = get_instance_files_for_deletion(instance_id)

            if instance_structure is None: # Implica que la instancia no existe o hubo error de BD
                return json.dumps({"status": "ERROR", "message": f"No se encontró la instancia de respaldo ID {instance_id} o no se pudieron obtener sus detalles."})

            cloud_files_to_delete = []
            if relative_file_paths: # Solo si hay archivos para borrar
                cloud_files_to_delete = [os.path.join(instance_structure, rel_path).replace("\\", "/") for rel_path in relative_file_paths]
            
            # Solicitar eliminación al cloud-service
            if cloud_files_to_delete: # Solo si hay archivos que eliminar en la nube
                print(f"[ServiceLogic] Solicitando eliminación de {len(cloud_files_to_delete)} archivo(s) al cloud-service...", flush=True)
                cloud_delete_payload = json.dumps({"files": cloud_files_to_delete})
                r_service_cloud, r_status_cloud, r_content_cloud = transact(BUS_HOST, BUS_PORT, "clcsv", f"delete_files|{cloud_delete_payload}")

                if r_status_cloud != "OK" or (r_content_cloud and r_content_cloud.strip().startswith("Error")):
                    # Si cloud-service devuelve un error en su contenido, incluso con status OK del bus.
                    err_msg = f"Fallo al eliminar archivos de la nube para la instancia ID {instance_id}. Respuesta: {r_content_cloud}"
                    print(f"[ServiceLogic] {err_msg}", flush=True)
                    return json.dumps({"status": "ERROR", "message": err_msg})
                print(f"[ServiceLogic] Respuesta de eliminación de nube: {r_content_cloud}", flush=True)
            else:
                print(f"[ServiceLogic] No hay archivos registrados en la BD para esta instancia (ID: {instance_id}) para eliminar de la nube.", flush=True)


            # Solicitar eliminación de copias locales al backup-service
            if relative_file_paths: # Solo si hay archivos que eliminar localmente
                print(f"[ServiceLogic] Solicitando eliminación de copias locales al backup-service...", flush=True)
                local_delete_payload = json.dumps({"structure": instance_structure, "relative_paths": relative_file_paths})
                r_service_local, r_status_local, r_content_local = transact(BUS_HOST, BUS_PORT, "bkpsv", f"delete_local_files|{local_delete_payload}")
                
                try:
                    local_delete_response = json.loads(r_content_local)
                    if r_status_local != "OK" or local_delete_response.get("status") != "OK":
                        err_msg = f"Fallo al eliminar copias locales para la instancia ID {instance_id}. Respuesta: {local_delete_response.get('message', r_content_local)}"
                        print(f"[ServiceLogic] {err_msg}", flush=True)

                        return json.dumps({"status": "ERROR", "message": err_msg + " (Los archivos en la nube podrían haber sido eliminados)."})
                    print(f"[ServiceLogic] Respuesta de eliminación local: {local_delete_response.get('message')}", flush=True)
                except json.JSONDecodeError:
                    err_msg = f"Respuesta inválida del backup-service al eliminar copias locales: {r_content_local}"
                    print(f"[ServiceLogic] {err_msg}", flush=True)
                    return json.dumps({"status": "ERROR", "message": err_msg + " (Los archivos en la nube podrían haber sido eliminados)."})
            else:
                print(f"[ServiceLogic] No hay archivos registrados en la BD para esta instancia (ID: {instance_id}) para eliminar localmente.", flush=True)

            # Eliminar metadatos de la base de datos
            print(f"[ServiceLogic] Eliminando metadatos de la instancia ID {instance_id} de la base de datos...", flush=True)
            success_db, message_db = delete_backup_instance_metadata(instance_id)
            if success_db:
                final_message = f"Respaldo ID {instance_id} y sus copias asociadas procesados para eliminación. DB: {message_db}"
                print(f"[ServiceLogic] {final_message}", flush=True)
                return json.dumps({"status": "OK", "message": final_message})
            else:
                # Las copias físicas podrían estar eliminadas pero los metadatos no.
                err_msg = f"Crítico: Las copias físicas del respaldo ID {instance_id} pueden haber sido eliminadas, pero falló la eliminación de sus metadatos de la BD: {message_db}"
                print(f"[ServiceLogic] {err_msg}", flush=True)
                return json.dumps({"status": "ERROR", "message": err_msg})

        except json.JSONDecodeError:
            return json.dumps({"status": "ERROR", "message": "Payload JSON malformado para delete_backup."})
        except Exception as e:
            print(f"[ServiceLogic] Error inesperado durante delete_backup para instancia ID {payload.get('instance_id', 'desconocida')}: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return json.dumps({"status": "ERROR", "message": f"Error interno del servidor al procesar delete_backup: {str(e)}"})
            
    else:
        return json.dumps({"status": "ERROR", "message": f"Comando '{command}' no reconocido."})

def main():
    """
    Punto de entrada principal. Inicia y mantiene el servicio en ejecución.
    """
    print(f"--- Iniciando lógica de negocio del servicio: {SERVICE_NAME} ---", flush=True)
    
    # Bucle infinito principal para garantizar la resiliencia del servicio.
    # Si la conexión con el bus se pierde, intentará reconectarse.
    while True:
        connector = ServiceConnector(BUS_HOST, BUS_PORT, SERVICE_NAME)
        try:
            # Establece la conexión y se registra en el bus.
            connector.connect_and_register()
            
            # Bucle secundario para atender múltiples peticiones en una misma conexión.
            while True:
                # Esperar un trabajo (llamada bloqueante).
                data_received = connector.wait_for_transaction()
                if data_received is None: 
                    print("[ServiceLogic] El bus cerró la conexión, se intentará reconectar.", flush=True)
                    break # Salir del bucle interno para forzar la reconexión.
                
                # Procesar el trabajo.
                response_data = process_request(data_received)
                
                # Enviar la respuesta.
                connector.send_response(response_data)

        except Exception as e:
            print(f"[ServiceLogic] Error: {e}. Reintentando en 5 segundos...", flush=True)
        finally:
            # Asegura que la conexión se cierre limpiamente antes de reintentar.
            connector.close()
        
        time.sleep(5)

if __name__ == "__main__":
    main()