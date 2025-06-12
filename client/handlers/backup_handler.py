import os
import base64
import json
from bus_connector import transact

def handle_create_backup(bus_host, bus_port):
    """
    Guía al usuario para seleccionar un archivo o un directorio y crear un respaldo
    utilizando un flujo transaccional.
    """
    print("\n--- Crear nuevo respaldo ---")
    path_input = input("Introduce la ruta completa del archivo o directorio a respaldar: ")
    
    if not os.path.exists(path_input):
        print("Error: La ruta especificada no existe.")
        return

    files_to_process_paths = []
    base_path_for_relative = ""

    if os.path.isfile(path_input):
        base_path_for_relative = os.path.dirname(path_input)
        files_to_process_paths.append(path_input)
    elif os.path.isdir(path_input):
        base_path_for_relative = path_input
        for root, _, files in os.walk(path_input):
            for filename in files:
                files_to_process_paths.append(os.path.join(root, filename))
    else:
        print("Error: La ruta no es ni un archivo ni un directorio válido.")
        return

    if not files_to_process_paths:
        print("No se encontraron archivos para respaldar en la ruta especificada.")
        return

    structure = input("Introduce una estructura de directorios para organizar el respaldo (ej. 'documentos/importantes'): ")
    target_service = "bkpsv"
    transaction_id = None

    try:
        # Iniciar transacción de respaldo
        print("\n[Cliente] Iniciando transacción de respaldo...")
        files_metadata_for_begin = []
        for full_path in files_to_process_paths:
            relative_path = os.path.relpath(full_path, base_path_for_relative)
            # Asegurarse de que las rutas relativas usen '/' como en Linux/web
            relative_path = relative_path.replace(os.sep, '/')
            files_metadata_for_begin.append({"relative_path": relative_path})

        begin_payload = {"structure": structure, "files_to_backup": files_metadata_for_begin}
        message_to_send = f"begin_backup|{json.dumps(begin_payload)}"
        
        r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)
        response = json.loads(r_content)

        if r_status != "OK" or response.get("status") != "OK":
            print(f"Error al iniciar respaldo: {response.get('message', r_content)}")
            return
        
        transaction_id = response.get("transaction_id")
        if not transaction_id:
            print(f"Error: No se recibió ID de transacción del servicio: {r_content}")
            return
        print(f"[Cliente] Transacción iniciada con ID: {transaction_id}")

        # Subir archivos
        print(f"\n[Cliente] Subiendo {len(files_to_process_paths)} archivo(s)...")
        for full_path in files_to_process_paths:
            relative_path = os.path.relpath(full_path, base_path_for_relative)
            relative_path = relative_path.replace(os.sep, '/') # Normalizar separadores
            
            with open(full_path, "rb") as f:
                file_bytes = f.read()
            content_b64 = base64.b64encode(file_bytes).decode('utf-8')
            
            upload_payload = {
                "transaction_id": transaction_id,
                "relative_path": relative_path,
                "content_b64": content_b64
            }
            message_to_send = f"upload_file|{json.dumps(upload_payload)}"

            # Verificar tamaño del mensaje individual (aproximado)
            if len(message_to_send.encode('utf-8')) > (99999 - len(target_service)):
                 print(f"\nError: El archivo '{relative_path}' es demasiado grande para ser enviado en una sola transacción de subida.")
                 print("Se abortará la transacción. Por favor, intente con archivos más pequeños o divida el contenido.")
                 # Intentar finalizar la transacción como fallida
                 if transaction_id:
                    end_payload = {"transaction_id": transaction_id, "abort": True}
                    message_to_send = f"end_backup|{json.dumps(end_payload)}"
                    transact(bus_host, bus_port, target_service, message_to_send) # Enviar y olvidar
                 return

            print(f"  Subiendo: {relative_path}...")
            r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)
            response = json.loads(r_content)

            if r_status != "OK" or response.get("status") != "OK":
                print(f"Error al subir archivo '{relative_path}': {response.get('message', r_content)}")
                print("Se abortará la transacción de respaldo.")
                return # Salir temprano si un archivo falla

        # Finalizar transacción de respaldo
        print("\n[Cliente] Finalizando transacción de respaldo...")
        end_payload = {"transaction_id": transaction_id}
        message_to_send = f"end_backup|{json.dumps(end_payload)}"
        
        r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)
        response = json.loads(r_content)

        print(f"\nRespuesta final del servicio '{r_service}' (Estado: {r_status})")
        print(f"Resultado: {response.get('message', r_content)}")

    except json.JSONDecodeError as e:
        print(f"Error al decodificar respuesta JSON del servicio: {e}")
    except Exception as e:
        print(f"Ocurrió un error durante el proceso de respaldo: {e}")
        # Si hay un error inesperado y se tiene un transaction_id, se puede intentar un end_backup con abort.
        if transaction_id and target_service:
            try:
                print("[Cliente] Intentando notificar al servicio sobre el aborto...")
                abort_payload = {"transaction_id": transaction_id, "abort": True}
                message_to_send = f"end_backup|{json.dumps(abort_payload)}"
                transact(bus_host, bus_port, target_service, message_to_send)
            except Exception as abort_e:
                print(f"Error al intentar notificar aborto: {abort_e}")