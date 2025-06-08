# admin-service/bus_connector.py
import socket

# Define un tamaño de búfer estándar para las lecturas de socket.
BUFFER_SIZE = 1024

def _receive_full_message(sock):
    """Lee un mensaje completo desde el socket, siguiendo el protocolo de prefijo de longitud.

    Esta función es bloqueante y no retornará hasta que el mensaje completo,
    según el largo especificado en los primeros 5 bytes, haya sido recibido.

    Args:
        sock (socket.socket): El socket conectado desde el cual leer.

    Returns:
        str | None: El contenido de los datos del mensaje (sin el nombre del servicio),
                    o None si la conexión es cerrada por el otro extremo.
    """
    try:
        # Leer los 5 bytes que definen la longitud del payload.
        raw_msg_len = sock.recv(5)
        if not raw_msg_len: return None
        msg_len = int(raw_msg_len)
        
        # Recolectar los trozos (chunks) del mensaje hasta completar el largo esperado.
        chunks = []
        bytes_received = 0
        while bytes_received < msg_len:
            chunk = sock.recv(min(msg_len - bytes_received, BUFFER_SIZE))
            if not chunk: raise ConnectionError("La conexión se cerró inesperadamente.")
            chunks.append(chunk)
            bytes_received += len(chunk)
        
        payload = b''.join(chunks).decode('utf-8')
        
        # Separar el nombre del servicio de los datos reales.
        # La lógica de negocio solo se interesa en los datos, no en el enrutamiento.
        data_content = payload[5:]
        return data_content

    except ValueError:
        raise ValueError("Error al decodificar la longitud del mensaje.")

def _send_message(sock, service, data):
    """Formatea un mensaje según el protocolo y lo envía a través del socket.

    Args:
        sock (socket.socket): El socket conectado al cual enviar el mensaje.
        service (str): El nombre del servicio de destino (5 caracteres).
        data (str): El contenido de los datos a enviar.
    """
    message_data = f"{service:5s}{data}"
    message = f"{len(message_data):05d}{message_data}".encode('utf-8')
    print(f"-> [BusConnector] Enviando: {message.decode()}", flush=True)
    sock.sendall(message)

class ServiceConnector:
    """Gestiona el ciclo de vida de la conexión de un servicio con el bus.
    
    Encapsula la lógica de conexión, registro ('sinit'), y el intercambio de
    mensajes para mantener el código de negocio limpio.

    Attributes:
        host (str): La dirección del host del bus.
        port (int): El puerto del bus.
        service_name (str): El nombre de este servicio (5 caracteres).
        sock (socket.socket | None): El objeto socket de la conexión activa.
    """
    def __init__(self, host, port, service_name):
        """Inicializa el conector del servicio.
        
        Args:
            host (str): La dirección del host del bus.
            port (int): El puerto del bus.
            service_name (str): El nombre de este servicio.
        """
        self.host = host
        self.port = port
        self.service_name = service_name
        self.sock = None

    def connect_and_register(self):
        """Realiza la conexión con el bus y registra el servicio con 'sinit'."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        print(f"[BusConnector] Conectado al bus en {self.host}:{self.port}", flush=True)
        
        # El primer paso después de conectar es siempre registrarse.
        _send_message(self.sock, "sinit", self.service_name)
        sinit_response = _receive_full_message(self.sock)
        
        print(f"<- [BusConnector] Respuesta de registro: {sinit_response}", flush=True)
        # Valida que el bus haya confirmado el registro.
        if "OK" not in sinit_response:
            raise ConnectionError("Fallo en el registro del servicio.")
        print(f"[BusConnector] Servicio '{self.service_name}' registrado.", flush=True)

    def wait_for_transaction(self):
        """Espera (bloquea) hasta recibir una transacción completa del bus."""
        print(f"[BusConnector] Esperando transacción para '{self.service_name}'...", flush=True)
        data = _receive_full_message(self.sock)
        print(f"<- [BusConnector] Datos recibidos para la lógica de negocio: {data}", flush=True)
        return data

    def send_response(self, response_data):
        """Envía la respuesta de la lógica de negocio de vuelta al bus."""
        _send_message(self.sock, self.service_name, response_data)

    def close(self):
        """Cierra la conexión del socket si está abierta."""
        if self.sock:
            self.sock.close()
            self.sock = None
            print("[BusConnector] Conexión cerrada.", flush=True)