# client/handlers/admin_handler.py
import json
import os
from bus_connector import transact

def handle_list_backups(bus_host, bus_port):
    """
    Solicita y muestra la lista de respaldos existentes, manejando paginación.
    """
    print("\n--- Solicitando lista de respaldos ---")
    target_service = "admsv"
    command = "listar"
    current_page = 1
    
    while True:
        print(f"\n--- Solicitando página {current_page} ---")
        page_payload_dict = {"page": current_page}
        page_payload_json = json.dumps(page_payload_dict)
        message_to_send = f"{command}|{page_payload_json}"
        
        print("--------------------")
        r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)
        print(f"Respuesta del servicio '{r_service}' (Estado: {r_status})")
        
        if r_status == "OK":
            print(r_content)
            # Verificar si el contenido indica que no hay más páginas o no hay respaldos
            if "No hay más respaldos registrados para esta página." in r_content or \
               "No hay respaldos registrados." in r_content and "Puede haber más páginas" not in r_content or \
               "No se encontraron detalles para los respaldos de esta página." in r_content:
                break # Salir si no hay más datos o es la única página sin datos
            
            if "Puede haber más páginas de resultados." not in r_content:
                 print("--- Fin de la lista de respaldos ---")
                 break

            ask_more = input("¿Desea ver la siguiente página? (s/n): ").strip().lower()
            if ask_more == 's':
                current_page += 1
            else:
                break
        else:
            print(f"Error al obtener la lista de respaldos: {r_content}")
            break
        print("--------------------")

def handle_configure_auto_backup(bus_host, bus_port):
    """
    Guía al usuario para configurar un nuevo trabajo de respaldo automático.
    """
    print("\n--- Configurar nuevo respaldo automático ---")
    try:
        job_name = input("Nombre descriptivo para el trabajo (ej. 'Documentos importantes'): ")
        source_path_input = input("Ruta del archivo o directorio a respaldar: ")
        destination_structure = input("Estructura de directorios para organizar el respaldo (ej. 'trabajo/proyectos'): ")
        frequency_hours_str = input("Frecuencia del respaldo en horas (ej. 24 para diario): ")

        if not all([job_name, source_path_input, destination_structure, frequency_hours_str]):
            print("Error: Todos los campos son obligatorios.")
            return

        # Validar la existencia de la ruta de origen
        if not os.path.exists(source_path_input):
            print(f"Error: La ruta de origen especificada '{source_path_input}' no existe o no es accesible desde el cliente.")
            return

        frequency_hours = int(frequency_hours_str)
        if frequency_hours <= 0:
            print("Error: La frecuencia debe ser un número positivo de horas.")
            return

        payload_dict = {
            "job_name": job_name,
            "source_path": source_path_input,
            "destination_structure": destination_structure,
            "frequency_hours": frequency_hours
        }
        payload_json = json.dumps(payload_dict)
        
        target_service = "admsv"
        command = "add_auto_job"
        message_to_send = f"{command}|{payload_json}"

        print("\nEnviando configuración de respaldo automático al servicio de administración...")
        print("-" * 20)
        r_service, r_status, r_content = transact(bus_host, bus_port, target_service, message_to_send)

        print(f"Respuesta del servicio '{r_service}' (Estado: {r_status})")
        try:
            response_data = json.loads(r_content)
            print(f"Mensaje: {response_data.get('message', r_content)}")
        except json.JSONDecodeError:
            print(f"Contenido: {r_content}")
        print("-" * 20)

    except ValueError:
        print("Error: La frecuencia debe ser un número entero de horas.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")