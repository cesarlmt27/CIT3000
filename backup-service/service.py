# backup-service/service.py
import os
import time
import base64
from bus_connector import ServiceConnector, transact
from db_handler import save_backup_records

BUS_HOST = os.getenv("BUS_HOST")
BUS_PORT = 5000
SERVICE_NAME = os.getenv("SERVICE_NAME", "bkpsv")

def process_request(data_received):
    """Orquesta la creación de un respaldo, con lógica de rollback."""
    local_path, secondary_path = None, None
    try:
        # Formato: nombre_archivo.txt|estructura_destino|contenido_base64
        filename, structure, file_content_b64 = data_received.split('|', 2)
        file_bytes = base64.b64decode(file_content_b64)
        
        # --- Crear copias locales (Temporal, se deben hacer modificaciones) ---
        local_path = f"/data/local_copy/{structure}/{filename}"
        secondary_path = f"/data/secondary_copy/{structure}/{filename}"
        
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(file_bytes)
        print(f"[ServiceLogic] Copia local creada: {local_path}")
        
        os.makedirs(os.path.dirname(secondary_path), exist_ok=True)
        with open(secondary_path, "wb") as f:
            f.write(file_bytes)
        print(f"[ServiceLogic] Copia secundaria creada: {secondary_path}")

        # --- Copiar en la nube ---
        cloud_payload = f"upload|{structure}/{filename}|{file_content_b64}"
        _, r_status, r_content = transact(BUS_HOST, BUS_PORT, "clcsv", cloud_payload)

        if r_status != "OK" or r_content.strip().startswith("Error"):
            raise Exception(f"Fallo en la copia a la nube: {r_content}")

        # --- Si no hay errores, guardar en la base de datos ---
        save_backup_records(filename, len(file_bytes), structure)

        return "Respaldo 3-2-1 completado exitosamente."

    except Exception as e:
        # Lógica de rollback en caso de error
        print(f"[ServiceLogic] ERROR: {e}. Iniciando rollback...", flush=True)
        # Intentar borrar los archivos creados si existen
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            print(f"[ServiceLogic] Rollback: Copia local eliminada: {local_path}", flush=True)
        if secondary_path and os.path.exists(secondary_path):
            os.remove(secondary_path)
            print(f"[ServiceLogic] Rollback: Copia secundaria eliminada: {secondary_path}", flush=True)
            
        return f"Proceso de respaldo falló y fue revertido. Causa: {e}"

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