# client/handlers/admin_handler.py
from bus_connector import transact

def handle_list_backups(bus_host, bus_port):
    """
    Prepara y envía la solicitud para listar los respaldos al 'admin-service'.
    
    Args:
        bus_host (str): La dirección del host del bus.
        bus_port (int): El puerto del bus.
    """
    print("\n--- Solicitando lista de respaldos ---")
    
    target_service = "admsv"
    message_to_send = "listar"

    print("-" * 20)
    r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)
    
    print(f"Respuesta del servicio '{r_service}' (Estado: {r_status})")
    print(r_content)
    print("-" * 20)