# client/handlers/restore_handler.py
import os
import json
import base64
from bus_connector import transact

def handle_restore_backup(bus_host, bus_port):
    print("\n--- Restaurar respaldo ---")
    try:
        instance_id_str = input("ID de la instancia de respaldo a restaurar: ")
        if not instance_id_str.isdigit():
            print("Error: El ID de la instancia debe ser un número.")
            return
        instance_id = int(instance_id_str)

        destination_base_path = input("Ruta base en este cliente donde se restaurarán los archivos (ej. /tmp/restored_files): ")
        if not destination_base_path:
            print("Error: La ruta de destino no puede estar vacía.")
            return
        
        os.makedirs(destination_base_path, exist_ok=True) # Crear directorio base si no existe

        # Obtener el plan de restauración (lista de archivos)
        print(f"\nObteniendo plan de restauración para la instancia ID {instance_id}...")
        plan_payload = json.dumps({"instance_id": instance_id})
        r_service, r_status, r_content = transact(bus_host, bus_port, "rstrv", f"get_restore_plan|{plan_payload}")

        if r_status != "OK":
            print(f"Error al obtener plan de restauración: {r_content}")
            return
        
        plan_data = json.loads(r_content)
        if plan_data.get("status") != "OK":
            print(f"Error del servicio de restauración: {plan_data.get('message', r_content)}")
            return

        files_to_restore = plan_data.get("files", [])
        instance_structure = plan_data.get("instance_structure", "")

        if not files_to_restore:
            print("No hay archivos para restaurar en esta instancia o la instancia está vacía.")
            return

        print(f"Se restaurarán {len(files_to_restore)} archivo(s) a '{destination_base_path}'.")
        
        successful_restores = 0
        failed_restores = 0

        # Solicitar cada archivo
        for file_meta in files_to_restore:
            relative_path = file_meta["relative_path"]
            original_hash = file_meta["hash"]
            
            print(f"\n  Solicitando restauración para: {relative_path} (Hash esperado: {original_hash[:8]}...)")
            file_req_payload = json.dumps({"instance_id": instance_id, "relative_path": relative_path})
            
            r_service_file, r_status_file, r_content_file = transact(bus_host, bus_port, "rstrv", f"request_file_restore|{file_req_payload}")

            if r_status_file != "OK":
                print(f"    Error en la comunicación con el servicio para el archivo: {r_content_file}")
                failed_restores += 1
                continue
            
            file_restore_data = json.loads(r_content_file)
            if file_restore_data.get("status") == "OK":
                content_b64 = file_restore_data.get("content_b64")
                source_medium = file_restore_data.get("source_medium", "desconocido")
                
                try:
                    file_bytes = base64.b64decode(content_b64)

                    # Escribir archivo en el cliente
                    client_file_path = os.path.join(destination_base_path, relative_path)
                    os.makedirs(os.path.dirname(client_file_path), exist_ok=True)
                    with open(client_file_path, "wb") as f:
                        f.write(file_bytes)
                    
                    print(f"    Éxito: '{relative_path}' restaurado desde '{source_medium}' a '{client_file_path}'.")
                    successful_restores += 1
                except Exception as e_write:
                    print(f"    Error al escribir/decodificar archivo '{relative_path}': {e_write}")
                    failed_restores += 1
            else:
                print(f"    Fallo al restaurar '{relative_path}': {file_restore_data.get('message', 'Error desconocido del servicio.')}")
                failed_restores += 1
        
        print("\n--- Resumen de la restauración ---")
        print(f"Archivos restaurados exitosamente: {successful_restores}")
        print(f"Archivos fallidos: {failed_restores}")

    except json.JSONDecodeError as e:
        print(f"Error al procesar respuesta del servicio (JSON inválido): {e}")
    except ConnectionRefusedError:
        print("Error: No se pudo conectar al bus de servicios. ¿Está en ejecución?")
    except Exception as e:
        print(f"Ocurrió un error inesperado durante la restauración: {e}")
        import traceback
        traceback.print_exc()