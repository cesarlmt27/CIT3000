# cloud-service/service.py
import os
import time
from bus_connector import ServiceConnector
from rclone_handler import create_remote, upload_file, download_file_content_as_base64

# --- Configuración del servicio ---
BUS_HOST = os.getenv("BUS_HOST")
BUS_PORT = 5000
SERVICE_NAME = os.getenv("SERVICE_NAME")
ACTIVE_PROVIDER_FILE = "/config/active_provider.info" # Ruta a un archivo para guardar el proveedor activo.

def set_active_provider(provider_name):
    """Guarda el nombre del proveedor activo en un archivo."""
    with open(ACTIVE_PROVIDER_FILE, "w") as f:
        f.write(provider_name)

def get_active_provider():
    """Lee el nombre del proveedor activo desde el archivo."""
    try:
        with open(ACTIVE_PROVIDER_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None

def process_request(data_received):
    """Procesa las solicitudes para el servicio de nube."""
    parts = data_received.split('|')
    command = parts[0]
    
    if command == "config":
        try:
            # Extracción de los datos: config|provider|user|pass
            _, provider, user, password = parts
            
            print(f"[ServiceLogic] Creando configuración de Rclone para '{provider}'...", flush=True)
            success, message = create_remote(provider, user, password)
            
            if success:
                set_active_provider(provider)
                
            return message
        except ValueError:
            return "Error: Formato de comando de configuración incorrecto."
        
    elif command == "upload":
        try:
            # Leer cuál es el proveedor que se configuró previamente.
            provider = get_active_provider()
            
            if not provider:
                return "Error: No hay ningún proveedor de nube configurado. Por favor, configúrelo primero."

            # Se construye el nombre del remote.
            remote_name = f"{provider}_remote"
            
            _, cloud_path, file_content_b64 = parts
            print(f"[ServiceLogic] Subiendo archivo a '{remote_name}:{cloud_path}'...", flush=True)
            success, message = upload_file(remote_name, cloud_path, file_content_b64)
            return message

        except ValueError:
            return "Error: Formato de comando de subida incorrecto."
    
    elif command == "download":
        try:
            # Espera: download|cloud_path_on_remote
            _, cloud_file_path = parts
            provider = get_active_provider()
            if not provider:
                return "Error: No hay proveedor de nube configurado."
            
            remote_name = f"{provider}_remote"
            print(f"[ServiceLogic] Solicitud de descarga para '{remote_name}:{cloud_file_path}'...", flush=True)
            
            success, content_or_error_msg = download_file_content_as_base64(remote_name, cloud_file_path)
            
            if success:
                # content_or_error_msg es el contenido en base64
                # El bus espera una cadena. El cliente que recibe debe saber que es base64.
                return content_or_error_msg 
            else:
                return f"Error: {content_or_error_msg}"
        except ValueError:
            return "Error: Formato de comando de descarga incorrecto."
        except Exception as e:
            print(f"[ServiceLogic] Error inesperado procesando descarga: {e}", flush=True)
            return f"Error: Error interno del servidor procesando descarga: {str(e)}"

    else:
        return f"Comando '{command}' no reconocido."

def main():
    """Punto de entrada principal que inicia y mantiene el servicio."""
    print(f"--- Iniciando lógica de negocio del servicio: {SERVICE_NAME} ---", flush=True)
    while True:
        connector = ServiceConnector(BUS_HOST, BUS_PORT, SERVICE_NAME)
        try:
            connector.connect_and_register()
            while True:
                data_received = connector.wait_for_transaction()
                if data_received is None:
                    print("[ServiceLogic] El bus cerró la conexión.", flush=True)
                    break
                response_data = process_request(data_received)
                connector.send_response(response_data)
        except Exception as e:
            print(f"[ServiceLogic] Error: {e}. Reintentando en 5 segundos...", flush=True)
        finally:
            connector.close()
        time.sleep(5)

if __name__ == "__main__":
    main()