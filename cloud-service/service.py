import os
import time
from bus_connector import ServiceConnector
from rclone_handler import create_remote

# --- Configuración del servicio ---
BUS_HOST = os.getenv("BUS_HOST")
BUS_PORT = 5000
SERVICE_NAME = os.getenv("SERVICE_NAME")

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
            return message

        except ValueError:
            return "Error: Formato de comando de configuración incorrecto."
    
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