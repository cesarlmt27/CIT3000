# client/client.py
"""
Punto de entrada principal para el cliente interactivo.

Este script maneja la interfaz de usuario en la terminal, captura las
entradas del usuario y utiliza el 'bus_connector' para comunicarse
con los servicios a través del bus.
"""
import os
from bus_connector import transact

# --- Configuración del cliente ---
# Lee la configuración desde las variables de entorno de Docker Compose.
BUS_HOST = os.getenv("BUS_HOST", "localhost")
BUS_PORT = 5000

if __name__ == "__main__":
    print("--- Cliente interactivo ---", flush=True)
    print("Escribe 'salir' para terminar.", flush=True)
    
    # Bucle principal para la interacción con el usuario.
    while True:
        user_input = input("\nPresiona Enter para enviar 'listar' al servicio 'admsv' (o escribe 'salir'): ")
        
        # Condición para terminar el programa.
        if user_input.lower() == 'salir':
            break
            
        # Define la solicitud a enviar. Actualmente, solo se envía un de prueba.
        target_service = "admsv"
        message_to_send = "listar"
        
        print("-" * 20, flush=True)
        
        # Llama a la función de alto nivel para realizar la transacción.
        # Toda la complejidad de la red está oculta en la función 'transact'.
        r_service, r_status, r_content = transact(BUS_HOST, BUS_PORT, target_service, message_to_send)
        
        # Muestra al usuario la respuesta ya procesada y clasificada.
        print(f"Respuesta del servicio '{r_service}'", flush=True)
        print(f"Estado: {r_status}", flush=True)
        print(f"Contenido: {r_content}", flush=True)
        print("-" * 20, flush=True)

    print("Cliente terminado.", flush=True)