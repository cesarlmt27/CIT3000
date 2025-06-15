import os
from handlers.cloud_handler import handle_cloud_config
from handlers.admin_handler import handle_list_backups, handle_configure_auto_backup # Añadir import
from handlers.backup_handler import handle_create_backup

# --- Configuración del cliente ---
BUS_HOST = os.getenv("BUS_HOST", "localhost")
BUS_PORT = 5000

def show_menu():
    """Imprime el menú principal de opciones y retorna la elección del usuario."""
    print("\n--- Menú principal del sistema de respaldo ---")
    print("1. Configurar proveedor de nube")
    print("2. Listar respaldos existentes")
    print("3. Crear nuevo respaldo (manual)")
    print("4. Configurar nuevo respaldo automático")
    print("9. Salir")
    return input("Selecciona una opción: ")

def run_interactive_mode(bus_host, bus_port):
    """Ejecuta el bucle del menú interactivo para el usuario."""
    print("--- Cliente interactivo ---", flush=True)
    print("Presiona Ctrl+C o ingresa '9' para salir.", flush=True)
    
    try:
        while True:
            choice = show_menu()
            
            if choice == '1':
                handle_cloud_config(bus_host, bus_port)
            elif choice == '2':
                handle_list_backups(bus_host, bus_port)
            elif choice == '3':
                handle_create_backup(bus_host, bus_port)
            elif choice == '4':
                handle_configure_auto_backup(bus_host, bus_port)
            elif choice == '9':
                print("Cliente terminado por el usuario.", flush=True)
                break
            else:
                print("Opción no válida, por favor intenta de nuevo.", flush=True)
    except KeyboardInterrupt:
        print("\nCliente terminado por interrupción (Ctrl+C).", flush=True)
    except Exception as e:
        print(f"\nOcurrió un error inesperado en el menú interactivo: {e}", flush=True)
    finally:
        print("Cerrando cliente...", flush=True)


if __name__ == "__main__":
    print("[MainClient] Iniciando cliente interactivo...", flush=True)

    run_interactive_mode(BUS_HOST, BUS_PORT)

    print("[MainClient] Saliendo del cliente interactivo...", flush=True)