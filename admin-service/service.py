import os
import time
from bus_connector import ServiceConnector

# --- Configuración del servicio ---
BUS_HOST = os.getenv("BUS_HOST", "localhost")
BUS_PORT = 5000
SERVICE_NAME = os.getenv("SERVICE_NAME", "admsv")

def process_request(data_received):
    """
    Aquí vive la lógica de negocio.
    Por ahora, es muy simple.
    """
    if data_received == "listar":
        return f"Respuesta procesada exitosamente por {SERVICE_NAME}"
    else:
        return f"Comando '{data_received}' no reconocido."

def main():
    print(f"--- Iniciando lógica de negocio del servicio: {SERVICE_NAME} ---", flush=True)
    
    while True: # Bucle para reintentar la conexión si se pierde
        connector = ServiceConnector(BUS_HOST, BUS_PORT, SERVICE_NAME)
        try:
            connector.connect_and_register()
            
            while True:
                # Esperar un trabajo del bus
                data_received = connector.wait_for_transaction()
                if data_received is None: break
                
                # Procesar el trabajo y obtener una respuesta
                response_data = process_request(data_received)
                
                # Enviar la respuesta
                connector.send_response(response_data)

        except Exception as e:
            print(f"[ServiceLogic] Error: {e}. Reintentando en 5 segundos...", flush=True)
        finally:
            connector.close()
        
        time.sleep(5)

if __name__ == "__main__":
    main()