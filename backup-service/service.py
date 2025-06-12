import os
import time
import base64
import json
import hashlib
import uuid
from bus_connector import ServiceConnector, transact
from db_handler import save_backup_records

BUS_HOST = os.getenv("BUS_HOST")
BUS_PORT = 5000
SERVICE_NAME = os.getenv("SERVICE_NAME", "bkpsv")

# Diccionario para manejar transacciones activas
active_transactions = {}

def cleanup_temp_files(temp_files_list):
    """Elimina una lista de archivos temporales."""
    print(f"[ServiceLogic] Limpiando {len(temp_files_list)} archivos temporales...", flush=True)
    for f_path in temp_files_list:
        try:
            if os.path.exists(f_path):
                os.remove(f_path)
                print(f"  Eliminado: {f_path}", flush=True)
        except Exception as e:
            print(f"  Error al eliminar {f_path}: {e}", flush=True)

def process_request(data_received):
    """Maneja los comandos del flujo transaccional de respaldo."""
    try:
        command, json_payload_str = data_received.split('|', 1)
        payload = json.loads(json_payload_str)
    except ValueError:
        return json.dumps({"status": "ERROR", "message": "Solicitud malformada. No se pudo separar comando y payload JSON."})
    except json.JSONDecodeError:
        return json.dumps({"status": "ERROR", "message": "Payload JSON malformado."})

    if command == "begin_backup":
        try:
            structure = payload['structure']
            files_to_backup = payload['files_to_backup'] # Lista de {"relative_path": "..."}
            
            tx_id = uuid.uuid4().hex
            active_transactions[tx_id] = {
                "structure": structure,
                "expected_files": set(f['relative_path'] for f in files_to_backup),
                "processed_files_db_meta": [],
                "temp_files_on_disk": [],
                "status": "pending"
            }
            print(f"[ServiceLogic] Transacción {tx_id} iniciada para {len(files_to_backup)} archivos.", flush=True)
            return json.dumps({"status": "OK", "transaction_id": tx_id})
        except Exception as e:
            return json.dumps({"status": "ERROR", "message": f"Error al iniciar respaldo: {str(e)}"})

    elif command == "upload_file":
        tx_id = payload.get('transaction_id')
        tx_data = active_transactions.get(tx_id)

        if not tx_data:
            return json.dumps({"status": "ERROR", "message": f"ID de transacción '{tx_id}' no encontrado o inválido."})
        if tx_data["status"] == "failed":
            return json.dumps({"status": "ERROR", "message": f"Transacción '{tx_id}' ya está marcada como fallida."})

        relative_path = payload['relative_path']
        if relative_path not in tx_data["expected_files"]:
             tx_data["status"] = "failed" # Marcar como fallida si se recibe archivo inesperado
             return json.dumps({"status": "ERROR", "message": f"Archivo '{relative_path}' no esperado en la transacción '{tx_id}'."})

        file_content_b64 = payload['content_b64']
        created_files_for_this_upload = []
        try:
            file_bytes = base64.b64decode(file_content_b64)
            file_hash = hashlib.sha256(file_bytes).hexdigest()
            file_size = len(file_bytes)

            # Asegurar que las rutas usen separadores internos consistentes
            safe_relative_path = relative_path.replace("\\", "/")
            base_backup_path = tx_data['structure'].replace("\\", "/")
            
            # Crear copias locales
            # Usar os.path.join para construir rutas de forma segura
            local_copy_dir = os.path.join("/data/local_copy", base_backup_path, os.path.dirname(safe_relative_path))
            local_path = os.path.join(local_copy_dir, os.path.basename(safe_relative_path))
            
            secondary_copy_dir = os.path.join("/data/secondary_copy", base_backup_path, os.path.dirname(safe_relative_path))
            secondary_path = os.path.join(secondary_copy_dir, os.path.basename(safe_relative_path))

            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f: f.write(file_bytes)
            created_files_for_this_upload.append(local_path)

            os.makedirs(os.path.dirname(secondary_path), exist_ok=True)
            with open(secondary_path, "wb") as f: f.write(file_bytes)
            created_files_for_this_upload.append(secondary_path)
            
            # Llamar al servicio de nube
            cloud_target_path = os.path.join(base_backup_path, safe_relative_path).replace("\\", "/")
            cloud_payload_str = f"upload|{cloud_target_path}|{file_content_b64}"
            _, r_status, r_content = transact(BUS_HOST, BUS_PORT, "clcsv", cloud_payload_str)
            
            if r_status != "OK" or (r_content and r_content.strip().startswith("Error")): # El bus puede no devolver OK en r_status
                # Si la subida a la nube falla, la transacción entera falla.
                tx_data["status"] = "failed"
                cleanup_temp_files(created_files_for_this_upload) # Limpiar solo los de este intento
                return json.dumps({"status": "ERROR", "message": f"Fallo en la copia a la nube para '{relative_path}': {r_content}"})

            tx_data["processed_files_db_meta"].append({
                "relative_path": relative_path, # Usar la original que el cliente envió
                "hash": file_hash,
                "size": file_size
            })
            tx_data["temp_files_on_disk"].extend(created_files_for_this_upload)
            tx_data["expected_files"].remove(relative_path)
            
            print(f"[ServiceLogic] Archivo '{relative_path}' procesado para tx {tx_id}.", flush=True)
            return json.dumps({"status": "OK", "file_processed": relative_path})

        except Exception as e:
            tx_data["status"] = "failed"
            cleanup_temp_files(created_files_for_this_upload) # Limpiar si se crearon antes del error
            return json.dumps({"status": "ERROR", "message": f"Error procesando archivo '{relative_path}': {str(e)}"})

    elif command == "end_backup":
        tx_id = payload.get('transaction_id')
        # Pop para remover la transacción, ya sea éxito o fallo, se finaliza.
        tx_data = active_transactions.pop(tx_id, None) 

        if not tx_data:
            return json.dumps({"status": "ERROR", "message": f"ID de transacción '{tx_id}' no encontrado para finalizar."})

        # Verificar si el cliente envió una señal de aborto explícita
        client_aborted = payload.get("abort", False)

        if client_aborted:
            tx_data["status"] = "failed"
            print(f"[ServiceLogic] Transacción {tx_id} abortada por el cliente.", flush=True)


        if tx_data["status"] == "failed" or len(tx_data["expected_files"]) > 0:
            reason = "marcada como fallida" if tx_data["status"] == "failed" else f"faltan {len(tx_data['expected_files'])} archivos por subir"
            print(f"[ServiceLogic] Transacción {tx_id} falló ({reason}). Iniciando rollback...", flush=True)
            cleanup_temp_files(tx_data["temp_files_on_disk"])
            return json.dumps({"status": "ERROR", "message": f"Proceso de respaldo falló y fue revertido. Causa: {reason}."})
        
        try:
            if not tx_data["processed_files_db_meta"]:
                 # Esto podría pasar si no se subió ningún archivo con éxito pero la tx no se marcó como failed
                 print(f"[ServiceLogic] Transacción {tx_id} finalizada sin archivos procesados para la BD.", flush=True)
                 cleanup_temp_files(tx_data["temp_files_on_disk"]) # Limpiar por si acaso
                 return json.dumps({"status": "OK", "message": "Respaldo finalizado, pero no se procesaron archivos para guardar en BD."})

            save_backup_records(tx_data["structure"], tx_data["processed_files_db_meta"])
            # Los archivos temporales ya no son "temporales" si la BD se actualizó, son las copias locales.
            # No se borran en caso de éxito.
            print(f"[ServiceLogic] Transacción {tx_id} completada exitosamente.", flush=True)
            return json.dumps({"status": "OK", "message": f"Respaldo completado exitosamente ({len(tx_data['processed_files_db_meta'])} archivos)."})
        except Exception as e:
            print(f"[ServiceLogic] Error al guardar en BD para tx {tx_id}, revirtiendo: {e}", flush=True)
            cleanup_temp_files(tx_data["temp_files_on_disk"])
            # Truncar mensaje de error para que no exceda el límite del payload.
            error_message = str(e)
            detailed_error = f"Error al guardar en BD, cambios en disco revertidos. Causa: {error_message[:100]}"
            return json.dumps({"status": "ERROR", "message": detailed_error})
    else:
        return json.dumps({"status": "ERROR", "message": f"Comando '{command}' no reconocido."})

def main():
    """
    Punto de entrada principal. Inicia y mantiene el servicio en ejecución.
    """
    print(f"--- Iniciando lógica de negocio del servicio: {SERVICE_NAME} ---", flush=True)
    
    while True:
        connector = ServiceConnector(BUS_HOST, BUS_PORT, SERVICE_NAME)
        try:
            connector.connect_and_register()
            while True:
                data_received = connector.wait_for_transaction()
                if data_received is None: 
                    print("[ServiceLogic] El bus cerró la conexión, se intentará reconectar.", flush=True)
                    break
                
                response_data_json = process_request(data_received)
                connector.send_response(response_data_json)

        except Exception as e:
            print(f"[ServiceLogic] Error en el bucle principal: {e}. Reintentando en 5 segundos...", flush=True)
        finally:
            connector.close()
        
        time.sleep(5)

if __name__ == "__main__":
    main()