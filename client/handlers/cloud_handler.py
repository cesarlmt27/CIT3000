# client/handlers/cloud_handler.py
import getpass
from bus_connector import transact

def handle_cloud_config(bus_host, bus_port):
    """
    Guía al usuario para configurar la cuenta.

    Args:
        bus_host (str): La dirección del host del bus.
        bus_port (int): El puerto del bus.
    """
    print("\n--- Configurar proveedor de nube: Mega ---")
    
    provider = "mega"

    # Solicita las credenciales al usuario.
    email = input(f"Introduce tu email de {provider}: ")
    # getpass permite escribir la contraseña sin que se muestre en la terminal.
    password = getpass.getpass(f"Introduce tu contraseña de {provider}: ")

    if not email or not password:
        print("El email y la contraseña no pueden estar vacíos.")
        return
    
    # Prepara el mensaje para el 'cloud-service'.
    # Formato: comando|proveedor|email|contraseña
    message_to_send = f"config|{provider}|{email}|{password}"
    target_service = "clcsv"

    print("\nEnviando configuración al servicio de nube...")
    print("-" * 20)
    r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)

    # Muestra la respuesta del servicio al usuario.
    # print(f"Respuesta del servicio '{r_service}' (Estado: {r_status})")
    print(f"Contenido: {r_content}")
    print("-" * 20)