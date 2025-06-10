# client/handlers/backup_handler.py
import os
import base64
import json
from bus_connector import transact

def handle_create_backup(bus_host, bus_port):
    """
    Guía al usuario para seleccionar un archivo o un directorio y crear un respaldo.
    """
    print("\n--- Crear nuevo respaldo ---")
    path_input = input("Introduce la ruta completa del archivo o directorio a respaldar: ")
    
    if not os.path.exists(path_input):
        print("Error: La ruta especificada no existe.")
        return

    files_to_process = []
    base_path = ""

    # Detrerminar si es un archivo o un directorio
    if os.path.isfile(path_input):
        print("Se ha detectado un archivo individual.")
        # Si es un archivo, el path base es su directorio contenedor
        base_path = os.path.dirname(path_input)
        files_to_process.append(path_input)
    elif os.path.isdir(path_input):
        print("Se ha detectado un directorio. Escaneando...")
        # Si es un directorio, ese es el path base
        base_path = path_input
        # Recorrer el árbol de directorios para encontrar todos los archivos
        for root, dirs, files in os.walk(path_input):
            for filename in files:
                files_to_process.append(os.path.join(root, filename))
    else:
        print("Error: La ruta no es ni un archivo ni un directorio válido.")
        return

    structure = input("Introduce una estructura de directorios para organizar el respaldo (ej. 'documentos/importantes'): ")
    
    try:
        files_for_payload = []
        for full_path in files_to_process:
            # Calcular la ruta relativa para mantener la estructura de carpetas
            relative_path = os.path.relpath(full_path, base_path)
            print(f"  Añadiendo a la lista: {relative_path}")
            
            with open(full_path, "rb") as f:
                file_bytes = f.read()
            
            # Codificar el contenido del archivo a base64
            content_b64 = base64.b64encode(file_bytes).decode('utf-8')
            
            files_for_payload.append({
                "relative_path": relative_path,
                "content_b64": content_b64
            })

        if not files_for_payload:
            print("No se encontraron archivos para respaldar.")
            return
            
        payload = {
            "structure": structure,
            "files": files_for_payload
        }
        
        message_to_send = json.dumps(payload)
        target_service = "bkpsv"

        if len(message_to_send.encode('utf-8')) > (99999 - len(target_service)):
            print(f"\nError: El directorio es demasiado grande para ser enviado en una sola transacción.")
            print(f"Tamaño del payload: {len(message_to_send.encode('utf-8'))} bytes. límite del payload: 99999 bytes.")
            return

        print(f"\nEnviando respaldo ({len(files_for_payload)} archivos) para proceso 3-2-1...")
        r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)

        print(f"\nRespuesta del servicio '{r_service}' (Estado: {r_status})")
        print(f"Contenido: {r_content}")

    except Exception as e:
        print(f"Ocurrió un error al procesar la ruta: {e}")