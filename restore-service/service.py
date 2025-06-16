import os
import json
import hashlib
import base64
import time
from bus_connector import ServiceConnector, transact
from db_handler import get_backup_instance_details, get_files_for_instance

BUS_HOST = os.getenv("BUS_HOST")
BUS_PORT = 5000
SERVICE_NAME = os.getenv("SERVICE_NAME", "rstrv")

# Rutas base dentro del contenedor donde se montan los volúmenes de respaldo
PRIMARY_SOURCE_BASE = "/sources/primary"
SECONDARY_SOURCE_BASE = "/sources/secondary"

def verify_hash(content_bytes, expected_hash):
    """Calcula el hash SHA256 del contenido y lo compara con el esperado."""
    current_hash = hashlib.sha256(content_bytes).hexdigest()
    return current_hash == expected_hash

def attempt_restore_from_path(full_path, expected_hash):
    """Intenta leer un archivo desde una ruta, verifica su hash y devuelve su contenido en base64."""
    if os.path.exists(full_path):
        try:
            with open(full_path, "rb") as f:
                content_bytes = f.read()
            if verify_hash(content_bytes, expected_hash):
                return True, base64.b64encode(content_bytes).decode('utf-8')
            else:
                print(f"[RestoreService] Fallo de hash para {full_path}", flush=True)
                return False, "hash_mismatch"
        except Exception as e:
            print(f"[RestoreService] Error leyendo {full_path}: {e}", flush=True)
            return False, f"read_error: {str(e)}"
    return False, "not_found"

def attempt_restore_from_cloud(cloud_service_name, cloud_path, expected_hash):
    """Intenta restaurar desde la nube a través del cloud-service."""
    print(f"[RestoreService] Intentando desde nube: {cloud_path}", flush=True)
    message_to_send = f"download|{cloud_path}"
    r_service, r_status, r_content = transact(BUS_HOST, BUS_PORT, cloud_service_name, message_to_send)

    if r_status == "OK" and not r_content.startswith("Error:"):
        try:
            content_bytes = base64.b64decode(r_content)
            if verify_hash(content_bytes, expected_hash):
                return True, r_content # r_content ya está en base64
            else:
                print(f"[RestoreService] Fallo de hash para archivo de nube {cloud_path}", flush=True)
                return False, "hash_mismatch_cloud"
        except Exception as e:
            print(f"[RestoreService] Error decodificando/hasheando contenido de nube para {cloud_path}: {e}", flush=True)
            return False, f"cloud_data_error: {str(e)}"
    else:
        print(f"[RestoreService] Error de cloud-service para {cloud_path}: {r_content}", flush=True)
        return False, f"cloud_service_error: {r_content}"


def process_request(data_received):
    try:
        command, json_payload_str = data_received.split('|', 1)
        payload = json.loads(json_payload_str)
    except ValueError:
        return json.dumps({"status": "ERROR", "message": "Solicitud malformada."})
    except json.JSONDecodeError:
        return json.dumps({"status": "ERROR", "message": "Payload JSON malformado."})

    if command == "get_restore_plan":
        instance_id = payload.get("instance_id")
        if instance_id is None:
            return json.dumps({"status": "ERROR", "message": "instance_id es requerido."})

        structure, _ = get_backup_instance_details(instance_id)
        if not structure:
            return json.dumps({"status": "ERROR", "message": f"No se encontró la instancia de respaldo {instance_id}."})
        
        files_metadata = get_files_for_instance(instance_id)
        if not files_metadata:
            return json.dumps({"status": "OK", "instance_structure": structure, "files": [], "message": "La instancia no contiene archivos."})
            
        return json.dumps({"status": "OK", "instance_structure": structure, "files": files_metadata})

    elif command == "request_file_restore":
        instance_id = payload.get("instance_id")
        relative_path = payload.get("relative_path")

        if not instance_id or not relative_path:
            return json.dumps({"status": "ERROR", "message": "instance_id y relative_path son requeridos."})

        instance_structure, _ = get_backup_instance_details(instance_id)
        if not instance_structure:
            return json.dumps({"status": "ERROR", "message": f"No se encontró la instancia de respaldo {instance_id}."})

        # Obtener el hash esperado para este archivo específico
        file_meta_list = get_files_for_instance(instance_id, specific_files_relative_paths=[relative_path])
        if not file_meta_list:
            return json.dumps({"status": "ERROR", "message": f"Archivo '{relative_path}' no encontrado en la instancia {instance_id}."})
        
        expected_hash = file_meta_list[0]["hash"]
        
        # Prioridad 1: Copia local primaria
        primary_path = os.path.join(PRIMARY_SOURCE_BASE, instance_structure, relative_path)
        print(f"[RestoreService] Intentando desde primaria: {primary_path}", flush=True)
        success, content_or_msg = attempt_restore_from_path(primary_path, expected_hash)
        if success:
            return json.dumps({"status": "OK", "relative_path": relative_path, "content_b64": content_or_msg, "source_medium": "local_primary", "original_hash": expected_hash})

        print(f"[RestoreService] Fallo desde primaria para {relative_path}: {content_or_msg}", flush=True)

        # Prioridad 2: Copia local secundaria
        secondary_path = os.path.join(SECONDARY_SOURCE_BASE, instance_structure, relative_path)
        print(f"[RestoreService] Intentando desde secundaria: {secondary_path}", flush=True)
        success, content_or_msg = attempt_restore_from_path(secondary_path, expected_hash)
        if success:
            return json.dumps({"status": "OK", "relative_path": relative_path, "content_b64": content_or_msg, "source_medium": "local_secondary", "original_hash": expected_hash})
        
        print(f"[RestoreService] Fallo desde secundaria para {relative_path}: {content_or_msg}", flush=True)
        
        # Prioridad 3: Nube
        cloud_path = os.path.join(instance_structure, relative_path).replace("\\", "/") # Asegurar separadores / para la nube
        success, content_or_msg = attempt_restore_from_cloud("clcsv", cloud_path, expected_hash)
        if success:
            return json.dumps({"status": "OK", "relative_path": relative_path, "content_b64": content_or_msg, "source_medium": "cloud", "original_hash": expected_hash})

        print(f"[RestoreService] Fallo desde nube para {relative_path}: {content_or_msg}", flush=True)
        return json.dumps({"status": "FAIL", "relative_path": relative_path, "message": f"No se pudo restaurar el archivo '{relative_path}' desde ninguna fuente o la verificación de integridad falló. Último error: {content_or_msg}"})
    
    else:
        return json.dumps({"status": "ERROR", "message": f"Comando '{command}' no reconocido."})


def main():
    print(f"--- Iniciando lógica de negocio del servicio: {SERVICE_NAME} ---", flush=True)
    while True:
        connector = ServiceConnector(BUS_HOST, BUS_PORT, SERVICE_NAME)
        try:
            connector.connect_and_register()
            while True:
                data_received = connector.wait_for_transaction()
                if data_received is None: 
                    print("[RestoreService] El bus cerró la conexión.", flush=True)
                    break 
                response_data_json = process_request(data_received)
                connector.send_response(response_data_json)
        except Exception as e:
            print(f"[RestoreService] Error en el bucle principal: {e}. Reintentando en 5 segundos...", flush=True)
        finally:
            connector.close()
        time.sleep(5)

if __name__ == "__main__":
    main()