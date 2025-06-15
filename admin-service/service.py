# admin-service/service.py
import os
import time
import json
from bus_connector import ServiceConnector
from db_handler import list_backup_instances, list_auto_backup_jobs, update_auto_job_timestamp, add_auto_backup_job

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