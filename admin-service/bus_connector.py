import socket

BUFFER_SIZE = 1024

def _receive_full_message(sock):
    """
    Función interna para leer un mensaje completo del socket.
    """
    try:
        raw_msg_len = sock.recv(5)
        if not raw_msg_len: return None
        msg_len = int(raw_msg_len)
        chunks = []
        bytes_received = 0
        while bytes_received < msg_len:
            chunk = sock.recv(min(msg_len - bytes_received, BUFFER_SIZE))
            if not chunk: raise ConnectionError("La conexión se cerró inesperadamente.")
            chunks.append(chunk)
            bytes_received += len(chunk)
        
        payload = b''.join(chunks).decode('utf-8')
        
        data_content = payload[5:]
        return data_content

    except ValueError:
        raise ValueError("Error al decodificar la longitud del mensaje.")

def _send_message(sock, service, data):
    """Función interna para formatear y enviar un mensaje."""
    message_data = f"{service:5s}{data}"
    message = f"{len(message_data):05d}{message_data}".encode('utf-8')
    print(f"-> [BusConnector] Enviando: {message.decode()}", flush=True)
    sock.sendall(message)

class ServiceConnector:
    """
    Clase que encapsula la conexión y comunicación de un servicio con el bus.
    """
    def __init__(self, host, port, service_name):
        self.host = host
        self.port = port
        self.service_name = service_name
        self.sock = None

    def connect_and_register(self):
        """Se conecta al bus y se registra con 'sinit'."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        print(f"[BusConnector] Conectado al bus en {self.host}:{self.port}", flush=True)
        
        _send_message(self.sock, "sinit", self.service_name)
        sinit_response = _receive_full_message(self.sock)
        
        print(f"<- [BusConnector] Respuesta de registro: {sinit_response}", flush=True)
        if "OK" not in sinit_response:
            raise ConnectionError("Fallo en el registro del servicio.")
        print(f"[BusConnector] Servicio '{self.service_name}' registrado.", flush=True)

    def wait_for_transaction(self):
        """Espera y recibe una transacción del bus."""
        print(f"[BusConnector] Esperando transacción para '{self.service_name}'...", flush=True)
        data = _receive_full_message(self.sock)
        print(f"<- [BusConnector] Datos recibidos para la lógica de negocio: {data}", flush=True)
        return data

    def send_response(self, response_data):
        """Envía una respuesta de vuelta al bus."""
        _send_message(self.sock, self.service_name, response_data)

    def close(self):
        """Cierra la conexión del socket."""
        if self.sock:
            self.sock.close()
            self.sock = None
            print("[BusConnector] Conexión cerrada.", flush=True)