# client/handlers/backup_handler.py
import os
import base64
from bus_connector import transact

def handle_create_backup(bus_host, bus_port):
    """Guía al usuario para seleccionar un archivo y crear un respaldo."""
    print("\n--- Crear nuevo respaldo ---")
    file_path = input("Introduce la ruta completa del archivo a respaldar: ")
    
    if not os.path.exists(file_path) or not os.path.isfile(file_path):
        print("Error: La ruta no existe o no es un archivo válido.")
        return

    structure = input("Introduce una estructura de directorios para organizar el respaldo (ej. 'documentos/importantes'): ")
    
    try:
        print(f"Leyendo el archivo '{os.path.basename(file_path)}'...")
        with open(file_path, "rb") as f:
            file_bytes = f.read()
        
        # Codificar el contenido del archivo en Base64
        file_content_b64 = base64.b64encode(file_bytes).decode('utf-8')
        filename = os.path.basename(file_path)

        # Formato: nombre_archivo|estructura|contenido_base64
        message_to_send = f"{filename}|{structure}|{file_content_b64}"
        target_service = "bkpsv"

        print("\nEnviando archivo para respaldo 3-2-1...")
        r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)

        print(f"\nRespuesta del servicio '{r_service}' (Estado: {r_status})")
        print(f"Contenido: {r_content}")

    except Exception as e:
        print(f"Ocurrió un error al procesar el archivo: {e}")