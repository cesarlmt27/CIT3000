import os
import base64
import json
from bus_connector import transact

def execute_backup(bus_host, bus_port, source_path, structure, auto_job_id=None):
    """
    Ejecuta el proceso de respaldo de forma no interactiva.
    Escanea la ruta de origen, procesa archivos y se comunica con el backup-service.
    """
    print(f"\n[BackupExecutor] Iniciando respaldo para: {source_path} -> {structure}")
    if auto_job_id:
        print(f"[BackupExecutor] Este es un respaldo automático del trabajo ID: {auto_job_id}")

    if not os.path.exists(source_path):
        print(f"[BackupExecutor] Error: La ruta de origen especificada '{source_path}' no existe o no es accesible desde el cliente.")
        return False

    files_to_process_paths = []
    base_path_for_relative = ""

    if os.path.isfile(source_path):
        base_path_for_relative = os.path.dirname(source_path)
        files_to_process_paths.append(source_path)
    elif os.path.isdir(source_path):
        base_path_for_relative = source_path
        for root, _, files in os.walk(source_path):
            for filename in files:
                files_to_process_paths.append(os.path.join(root, filename))
    else:
        print(f"[BackupExecutor] Error: La ruta '{source_path}' no es ni un archivo ni un directorio válido.")
        return False

    if not files_to_process_paths:
        print(f"[BackupExecutor] No se encontraron archivos para respaldar en '{source_path}'.")
        return True

    target_service = "bkpsv"
    transaction_id = None

    try:
        # Inicio del respaldo
        print(f"[BackupExecutor] Iniciando transacción de respaldo para {len(files_to_process_paths)} archivo(s)...")
        files_metadata_for_begin = []
        for full_path in files_to_process_paths:
            relative_path = os.path.relpath(full_path, base_path_for_relative)
            relative_path = relative_path.replace(os.sep, '/')
            files_metadata_for_begin.append({"relative_path": relative_path})

        begin_payload = {"structure": structure, "files_to_backup": files_metadata_for_begin}
        if auto_job_id is not None:
            begin_payload["auto_job_id"] = auto_job_id
        
        message_to_send = f"begin_backup|{json.dumps(begin_payload)}"
        
        r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)
        response = json.loads(r_content)

        if r_status != "OK" or response.get("status") != "OK":
            print(f"[BackupExecutor] Error al iniciar respaldo: {response.get('message', r_content)}")
            return False
        
        transaction_id = response.get("transaction_id")
        if not transaction_id:
            print(f"[BackupExecutor] Error: No se recibió ID de transacción del servicio: {r_content}")
            return False
        print(f"[BackupExecutor] Transacción iniciada con ID: {transaction_id}")

        # Subir archivos
        print(f"[BackupExecutor] Subiendo {len(files_to_process_paths)} archivo(s)...")
        for full_path in files_to_process_paths:
            relative_path = os.path.relpath(full_path, base_path_for_relative)
            relative_path = relative_path.replace(os.sep, '/')
            
            with open(full_path, "rb") as f:
                file_bytes = f.read()
            content_b64 = base64.b64encode(file_bytes).decode('utf-8')
            
            upload_payload = {
                "transaction_id": transaction_id,
                "relative_path": relative_path,
                "content_b64": content_b64
            }
            message_to_send = f"upload_file|{json.dumps(upload_payload)}"

            if len(message_to_send.encode('utf-8')) > (99999 - len(target_service)):
                 print(f"[BackupExecutor] Error: El archivo '{relative_path}' es demasiado grande.")
                 if transaction_id:
                    end_payload = {"transaction_id": transaction_id, "abort": True}
                    message_to_send = f"end_backup|{json.dumps(end_payload)}"
                    transact(bus_host, bus_port, target_service, message_to_send)
                 return False

            print(f"  Subiendo: {relative_path}...")
            r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)
            response = json.loads(r_content)

            if r_status != "OK" or response.get("status") != "OK":
                print(f"[BackupExecutor] Error al subir archivo '{relative_path}': {response.get('message', r_content)}")
                return False

        # Finalización del respaldo
        print("[BackupExecutor] Finalizando transacción de respaldo...")
        end_payload = {"transaction_id": transaction_id}
        message_to_send = f"end_backup|{json.dumps(end_payload)}"
        
        r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)
        response = json.loads(r_content)

        print(f"[BackupExecutor] Respuesta final del servicio '{r_service}' (Estado: {r_status})")
        print(f"Resultado: {response.get('message', r_content)}")
        return response.get("status") == "OK"

    except json.JSONDecodeError as e:
        print(f"[BackupExecutor] Error al decodificar respuesta JSON del servicio: {e}")
        return False
    except Exception as e:
        print(f"[BackupExecutor] Ocurrió un error durante el proceso de respaldo: {e}")
        if transaction_id and target_service:
            try:
                print("[BackupExecutor] Intentando notificar al servicio sobre el aborto...")
                abort_payload = {"transaction_id": transaction_id, "abort": True}
                message_to_send = f"end_backup|{json.dumps(abort_payload)}"
                transact(bus_host, bus_port, target_service, message_to_send)
            except Exception as abort_e:
                print(f"[BackupExecutor] Error al intentar notificar aborto: {abort_e}")
        return False

def handle_create_backup(bus_host, bus_port):
    """
    Guía al usuario para seleccionar un archivo o un directorio y crear un respaldo.
    """
    print("\n--- Crear nuevo respaldo (manual) ---")
    path_input = input("Introduce la ruta del archivo o directorio a respaldar: ")
    structure = input("Introduce una estructura de directorios para organizar el respaldo (ej. 'documentos/importantes'): ")

    if not path_input or not structure:
        print("La ruta y la estructura no pueden estar vacías.")
        return

    execute_backup(bus_host, bus_port, path_input, structure)