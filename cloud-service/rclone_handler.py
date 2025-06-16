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

def download_file_content_as_base64(remote_name, cloud_path):
    """
    Descarga un archivo desde la nube y devuelve su contenido como Base64.
    Lo hace copiando el archivo a una ubicación temporal en el contenedor del cloud-service,
    leyéndolo, y luego eliminándolo.
    """
    api_user = os.getenv("RCLONE_API_USER")
    api_pass = os.getenv("RCLONE_API_PASS")
    
    # Usar un nombre de archivo temporal único para evitar colisiones si hay descargas concurrentes
    # (aunque este servicio es de un solo hilo por ahora)
    temp_filename = f"temp_download_{os.path.basename(cloud_path)}_{os.urandom(4).hex()}"
    temp_local_download_path = f"/data/{temp_filename}" # /data es el volumen de rclone_data

    copy_payload = {
        "srcFs": f"{remote_name}:", # ej: "mega_remote:"
        "srcRemote": cloud_path,    # ej: "backups/documentos/file.txt"
        "dstFs": "/data/",          # Directorio local dentro del contenedor de cloud-service
        "dstRemote": temp_filename  # Nombre del archivo en /data
    }
    copy_endpoint = "http://localhost:5572/operations/copyfile"
    
    try:
        print(f"[RcloneHandler] Copiando desde nube: {remote_name}:{cloud_path} a local {temp_local_download_path}", flush=True)
        response = requests.post(copy_endpoint, auth=(api_user, api_pass), json=copy_payload)
        response.raise_for_status() 

        if os.path.exists(temp_local_download_path):
            with open(temp_local_download_path, "rb") as f:
                file_bytes = f.read()
            print(f"[RcloneHandler] Archivo leído desde {temp_local_download_path}, tamaño: {len(file_bytes)} bytes", flush=True)
            return True, base64.b64encode(file_bytes).decode('utf-8')
        else:
            print(f"[RcloneHandler] Error: Archivo no encontrado en {temp_local_download_path} después de la copia.", flush=True)
            return False, "Error: El archivo no se pudo copiar desde la nube al área temporal."

    except requests.exceptions.RequestException as e:
        error_text = e.response.text if e.response else str(e)
        print(f"[RcloneHandler] Error de API Rclone al descargar: {error_text}", flush=True)
        return False, f"Error de API Rclone al descargar: {error_text}"
    except Exception as e:
        print(f"[RcloneHandler] Error inesperado durante descarga de nube: {e}", flush=True)
        return False, f"Error inesperado durante la descarga desde la nube: {str(e)}"
    finally:
        if os.path.exists(temp_local_download_path):
            try:
                os.remove(temp_local_download_path)
                print(f"[RcloneHandler] Archivo temporal {temp_local_download_path} eliminado.", flush=True)
            except Exception as e_del:
                print(f"[RcloneHandler] Error al eliminar archivo temporal {temp_local_download_path}: {e_del}", flush=True)

def delete_file_from_remote(remote_name, cloud_path):
    """
    Elimina un archivo específico de la nube usando la API de Rclone.
    """
    api_endpoint = "http://localhost:5572/operations/deletefile"
    api_user = os.getenv("RCLONE_API_USER")
    api_pass = os.getenv("RCLONE_API_PASS")

    payload = {
        "fs": f"{remote_name}:",
        "remote": cloud_path
    }
    
    try:
        print(f"[RcloneHandler] Solicitando eliminación de nube: {remote_name}:{cloud_path}", flush=True)
        response = requests.post(api_endpoint, auth=(api_user, api_pass), json=payload)
        response.raise_for_status() # Lanza excepción para errores HTTP 4xx/5xx
        # Rclone devuelve un cuerpo vacío en éxito para deletefile
        print(f"[RcloneHandler] Archivo '{cloud_path}' eliminado exitosamente de '{remote_name}'.", flush=True)
        return True, f"Archivo '{cloud_path}' eliminado exitosamente de '{remote_name}'."
    except requests.exceptions.HTTPError as e:
        # Intentar obtener más detalles del error de Rclone si es posible
        error_details = e.response.text
        print(f"[RcloneHandler] Error HTTP de API Rclone al eliminar '{cloud_path}': {e.response.status_code} - {error_details}", flush=True)

        # Se asume que un error HTTP significa que no se pudo confirmar la eliminación.
        return False, f"Error de API Rclone al eliminar '{cloud_path}': {e.response.status_code} - {error_details}"
    except requests.exceptions.RequestException as e:
        print(f"[RcloneHandler] Error de comunicación con API Rclone al eliminar '{cloud_path}': {e}", flush=True)
        return False, f"Error de comunicación con API Rclone al eliminar '{cloud_path}': {str(e)}"
    except Exception as e:
        print(f"[RcloneHandler] Error inesperado durante eliminación de nube para '{cloud_path}': {e}", flush=True)
        return False, f"Error inesperado durante eliminación de nube para '{cloud_path}': {str(e)}"