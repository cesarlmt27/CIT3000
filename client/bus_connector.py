# client/bus_connector.py
import socket

# Define un tamaño de búfer estándar para las lecturas de socket.
BUFFER_SIZE = 1024

def _receive_full_message(sock):
    """Lee y parsea un mensaje completo desde el bus.

    La respuesta del bus para el cliente tiene el formato:
    Largo (5) | Servicio (5) | Status (2) | Datos (resto)

    Args:
        sock (socket.socket): El socket conectado desde el cual leer.

    Returns:
        tuple[str | None, str | None, str | None]: Una tupla conteniendo
            (nombre_servicio, estado, contenido). Retorna (None, None, None)
            si la conexión se cierra.
    """
    try:
        # Leer los 5 bytes de la longitud.
        raw_msg_len = sock.recv(5)
        if not raw_msg_len: return None, None, None
        msg_len = int(raw_msg_len.decode('utf-8'))
        
        # Recolectar los trozos (chunks) del payload.
        chunks = []
        bytes_received = 0
        while bytes_received < msg_len:
            chunk = sock.recv(min(msg_len - bytes_received, BUFFER_SIZE))
            if not chunk: raise ConnectionError("La conexión se cerró inesperadamente.")
            chunks.append(chunk)
            bytes_received += len(chunk)
            
        payload = b''.join(chunks).decode('utf-8')
        
        # Imprime la transacción cruda para depuración.
        print(f"<- RAW: {raw_msg_len.decode('utf-8')}{payload}", flush=True)
        
        # Parsear el payload en sus componentes.
        service_name = payload[:5]
        status = payload[5:7]
        content = payload[7:]
        
        return service_name, status, content
    except (ValueError, IndexError) as e:
        print(f"Error al parsear la respuesta del bus: {e}", flush=True)
        return "ERROR", "ER", "Trama corrupta"

def _format_and_send(sock, service, data):
    """Formatea y envía un mensaje al bus.

    Args:
        sock (socket.socket): El socket conectado.
        service (str): El servicio de destino.
        data (str): El contenido a enviar.
    """
    message_data = f"{service:5s}{data}"
    message = f"{len(message_data):05d}{message_data}".encode('utf-8')
    print(f"-> Enviando: {message.decode()}", flush=True)
    sock.sendall(message)

def transact(host, port, service_name, data_payload):
    """Realiza una transacción completa con el bus: conectar, enviar y recibir.

    Esta es la función principal que la lógica del cliente usará. Encapsula
    toda la complejidad de la comunicación en una sola llamada.

    Args:
        host (str): La dirección del host del bus.
        port (int): El puerto del bus.
        service_name (str): El servicio de destino para la solicitud.
        data_payload (str): Los datos que se enviarán al servicio.

    Returns:
        tuple: Una tupla con (nombre_servicio, estado, contenido) de la respuesta.
    """
    try:
        # 'with' asegura que el socket se cierre automáticamente al finalizar.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            _format_and_send(sock, service_name, data_payload)
            return _receive_full_message(sock)
    except Exception as e:
        print(f"[Transact] Ocurrió un error: {e}", flush=True)
        return "ERROR", "NK", str(e)