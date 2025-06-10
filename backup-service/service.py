# backup-service/service.py
import os
import time
import base64
import json
import hashlib
from bus_connector import ServiceConnector, transact
from db_handler import save_backup_records

BUS_HOST = os.getenv("BUS_HOST")
BUS_PORT = 5000
SERVICE_NAME = os.getenv("SERVICE_NAME", "bkpsv")

def process_request(data_received):
    """Orquesta la creación de un respaldo de directorio, con lógica de rollback."""
    created_files = []
    try:
        payload = json.loads(data_received)
        structure = payload['structure']
        files = payload['files']
        
        files_metadata_for_db = []

        for file_data in files:
            filename = file_data['relative_path']
            file_content_b64 = file_data['content_b64']
            
            # Decodificar Base64 para obtener los bytes originales
            file_bytes = base64.b64decode(file_content_b64)
            
            # Se calcula el hash del archivo
            file_hash = hashlib.sha256(file_bytes).hexdigest()
            print(f"[ServiceLogic] Hash para '{filename}': {file_hash}", flush=True)

            # Crear copias locales
            local_path = f"/data/local_copy/{structure}/{filename}"
            secondary_path = f"/data/secondary_copy/{structure}/{filename}"
            
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f: f.write(file_bytes)
            created_files.append(local_path)

            os.makedirs(os.path.dirname(secondary_path), exist_ok=True)
            with open(secondary_path, "wb") as f: f.write(file_bytes)
            created_files.append(secondary_path)

            # Llamar al servicio de nube
            cloud_payload = f"upload|{structure}/{filename}|{file_content_b64}"
            _, r_status, r_content = transact(BUS_HOST, BUS_PORT, "clcsv", cloud_payload)
            
            if r_status != "OK" or r_content.strip().startswith("Error"):
                raise Exception(f"Fallo en la copia a la nube para '{filename}': {r_content}")

            # Guardar metadatos para la inserción final en la BD
            files_metadata_for_db.append({
                "relative_path": filename,
                "hash": file_hash,
                "size": len(file_bytes)
            })

        # Guardar los registros en la base de datos, si no hay errores.
        save_backup_records(structure, files_metadata_for_db)

        return f"Respaldo de directorio completado exitosamente ({len(files)} archivos)."

    except Exception as e:
        # Lógica de rollback
        print(f"[ServiceLogic] ERROR: {e}. Iniciando rollback...", flush=True)
        for file_path in created_files:
            if os.path.exists(file_path):
                os.remove(file_path)
        # Truncar el mensaje de error para que no exceda el límite del payload.
        error_message = str(e)
        detailed_error = f"Proceso de respaldo falló y fue revertido. Causa: {error_message[:1000]}"
        return detailed_error

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