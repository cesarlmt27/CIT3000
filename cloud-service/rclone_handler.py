# cloud-service/rclone_handler.py
import requests
import os

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