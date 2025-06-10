# client/client.py
"""
Punto de entrada principal para el cliente interactivo.

Este script actúa como un despachador (dispatcher) que muestra un menú
y llama a los manejadores (handlers) apropiados para cada opción.
"""
import os
from handlers.cloud_handler import handle_cloud_config
from handlers.admin_handler import handle_list_backups
from handlers.backup_handler import handle_create_backup

# --- Configuración del cliente ---
BUS_HOST = os.getenv("BUS_HOST", "localhost")
BUS_PORT = 5000

def show_menu():
    """Imprime el menú principal de opciones y retorna la elección del usuario."""
    print("\n--- Menú principal del sistema de respaldo ---")
    print("1. Configurar proveedor de nube")
    print("2. Listar respaldos existentes")
    print("3. Crear nuevo respaldo")
    print("9. Salir")
    return input("Selecciona una opción: ")

if __name__ == "__main__":
    print("--- Cliente interactivo ---", flush=True)
    
    # Bucle principal del menú.
    while True:
        choice = show_menu()
        
        if choice == '1':
            # Llama a la función del handler de nube.
            handle_cloud_config(BUS_HOST, BUS_PORT)
        
        elif choice == '2':
            # Llama a la función del handler de administración de respaldos.
            handle_list_backups(BUS_HOST, BUS_PORT)

        elif choice == '3':
            # Llama a la función del handler de respaldo.
            handle_create_backup(BUS_HOST, BUS_PORT)
            
        elif choice == '9':
            print("Cliente terminado.", flush=True)
            break
            
        else:
            print("Opción no válida, por favor intenta de nuevo.", flush=True)