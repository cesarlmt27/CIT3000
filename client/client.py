import os
from bus_connector import transact

# --- Configuración del cliente ---
BUS_HOST = os.getenv("BUS_HOST", "localhost")
BUS_PORT = 5000

if __name__ == "__main__":
    print("--- Cliente interactivo ---", flush=True)
    print("Escribe 'salir' para terminar.", flush=True)
    
    while True:
        user_input = input("\nPresiona Enter para enviar 'listar' al servicio 'admsv' (o escribe 'salir'): ")
        
        if user_input.lower() == 'salir':
            break
            
        # Lógica de negocio del cliente: define qué enviar.
        target_service = "admsv"
        message_to_send = "listar"
        
        print("-" * 20, flush=True)
        
        # Llama a la función de la "librería" para hacer la transacción
        r_service, r_status, r_content = transact(BUS_HOST, BUS_PORT, target_service, message_to_send)
        
        # Muestra el resultado al usuario
        print(f"Respuesta del servicio '{r_service}'", flush=True)
        print(f"Estado: {r_status}", flush=True)
        print(f"Contenido: {r_content}", flush=True)
        print("-" * 20, flush=True)

    print("Cliente terminado.", flush=True)