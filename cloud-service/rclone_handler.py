# cloud-service/rclone_handler.py
import requests
import os
import base64

def create_remote(provider, user_creds, pass_creds):
    """
    Usa la API de Rclone para crear una nueva configuración de nube.
    """
    api_endpoint = "http://localhost:5572/config/create"
    
    # Lee las credenciales de la API desde el entorno
    api_user = os.getenv("RCLONE_API_USER")
    api_pass = os.getenv("RCLONE_API_PASS")
    
    remote_name = f"{provider}_remote" # ej: pcloud_remote, mega_remote

    # Rclone usa diferentes nombres de parámetro para las credenciales según el proveedor.
    if provider == "mega":
        parameters = {"user": user_creds, "pass": pass_creds}
    else:
        return False, f"Proveedor '{provider}' no soportado por este handler."

    payload = {
        "name": remote_name,
        "type": provider,
        "parameters": parameters
    }
    
    try:
        response = requests.post(api_endpoint, auth=(api_user, api_pass), json=payload)
        response.raise_for_status()
        return True, f"Configuración para '{remote_name}' creada exitosamente."
    except requests.exceptions.RequestException as e:
        return False, f"Error al comunicarse con la API de Rclone: {e.response.text if e.response else e}"

def upload_file(remote_name, cloud_path, file_content_b64):
    """
    Sube un archivo a la nube usando la API de Rclone.

    Para hacer esto, primero se guarda el archivo temporalmente en el
    contenedor, luego se le pide a Rclone que lo copie, y finalmente
    se borra el archivo temporal.
    """
    api_endpoint = "http://localhost:5572/operations/copyfile"
    api_user = os.getenv("RCLONE_API_USER")
    api_pass = os.getenv("RCLONE_API_PASS")
    
    # Extraer solo el nombre del archivo de la ruta completa en la nube
    filename = os.path.basename(cloud_path)
    # Ruta temporal donde se guarda el archivo dentro del contenedor
    temp_local_path = f"/data/{filename}"

    try:
        # Decodificar Base64 y guardar el archivo temporalmente
        file_bytes = base64.b64decode(file_content_b64)
        with open(temp_local_path, "wb") as f:
            f.write(file_bytes)

        # Construir el payload para la API de Rclone
        payload = {
            "srcFs": "/data",           # El sistema de archivos de origen es la carpeta /data
            "srcRemote": filename,      # El nombre del archivo a copiar desde /data
            "dstFs": f"{remote_name}:", # El remote de destino (ej: "mega_remote:")
            "dstRemote": cloud_path     # La ruta completa de destino en la nube
        }
        
        # Llamar a la API de Rclone
        response = requests.post(api_endpoint, auth=(api_user, api_pass), json=payload)
        response.raise_for_status()

        return True, f"Archivo '{filename}' subido exitosamente a '{cloud_path}'."

    except Exception as e:
        return False, f"Error durante la subida del archivo: {e}"
    finally:
        # Asegurarse de borrar el archivo temporal
        if os.path.exists(temp_local_path):
            os.remove(temp_local_path)