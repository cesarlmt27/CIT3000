import socket

BUFFER_SIZE = 1024

def _receive_full_message(sock):
    """Función interna para leer y parsear un mensaje completo."""
    try:
        raw_msg_len = sock.recv(5)
        if not raw_msg_len: return None, None, None
        msg_len = int(raw_msg_len.decode('utf-8'))
        
        chunks = []
        bytes_received = 0
        while bytes_received < msg_len:
            chunk = sock.recv(min(msg_len - bytes_received, BUFFER_SIZE))
            if not chunk: raise ConnectionError("La conexión se cerró inesperadamente.")
            chunks.append(chunk)
            bytes_received += len(chunk)
            
        payload = b''.join(chunks).decode('utf-8')
        
        print(f"<- RAW: {raw_msg_len.decode('utf-8')}{payload}", flush=True)
        
        service_name = payload[:5]
        status = payload[5:7]
        content = payload[7:]
        
        return service_name, status, content
    except (ValueError, IndexError):
        print("Error al parsear la respuesta del bus", flush=True)
        return "ERROR", "ER", "Trama corrupta"

def _format_and_send(sock, service, data):
    """Función interna para formatear y enviar un mensaje."""
    message_data = f"{service:5s}{data}"
    message = f"{len(message_data):05d}{message_data}".encode('utf-8')
    print(f"-> Enviando: {message.decode()}", flush=True)
    sock.sendall(message)

def transact(host, port, service_name, data_payload):
    """
    Realiza una transacción completa con el bus: conectar, enviar y recibir.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            _format_and_send(sock, service_name, data_payload)
            return _receive_full_message(sock)
    except Exception as e:
        print(f"[Transact] Ocurrió un error: {e}", flush=True)
        return "ERROR", "NK", str(e)