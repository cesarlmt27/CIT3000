# admin-service/service.py
import os
import time
from bus_connector import ServiceConnector
from db_handler import list_backup_instances

# --- Configuración del servicio ---
BUS_HOST = os.getenv("BUS_HOST")
BUS_PORT = 5000
SERVICE_NAME = os.getenv("SERVICE_NAME")

def process_request(data_received):
    """
    Contiene la lógica de negocio principal del servicio.
    
    Esta función recibe los datos ya limpios del conector del bus y
    decide qué acción realizar.

    Args:
        data_received (str): El comando o dato recibido del cliente.

    Returns:
        str: La respuesta que se enviará de vuelta al cliente.
    """
    print(f"[ServiceLogic] Procesando comando: '{data_received}'")
    
    if data_received == "listar":
        return list_backup_instances()
    else:
        return f"Comando '{data_received}' no reconocido."

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