# common_package/bus_connector/connector.py
import socket

# Define un tamaño de búfer estándar para las lecturas de socket.
BUFFER_SIZE = 1024


# --- FUNCIONES DE PROTOCOLO DE BAJO NIVEL (PRIVADAS) ---

def _read_payload_from_socket(sock):
    """
    Lee un payload completo desde el socket.

    Esta es la función central y unificada para la recepción. Lee los 5 bytes
    de longitud y luego recolecta los datos hasta recibir el mensaje completo.
    No interpreta el contenido, solo devuelve el payload crudo.

    Args:
        sock (socket.socket): El socket conectado desde el cual leer.

    Returns:
        str | None: El payload completo como una cadena de texto, o None si
                    la conexión se cierra.
    """
    try:
        # Leer los 5 bytes que definen la longitud del payload.
        raw_msg_len = sock.recv(5)
        if not raw_msg_len: 
            raise ConnectionError("La conexión fue cerrada por el otro extremo.")
        msg_len = int(raw_msg_len)
        
        # Recolectar los trozos (chunks) del mensaje hasta completar el largo esperado.
        chunks = []
        bytes_received = 0
        while bytes_received < msg_len:
            chunk = sock.recv(min(msg_len - bytes_received, BUFFER_SIZE))
            if not chunk: raise ConnectionError("La conexión se cerró inesperadamente.")
            chunks.append(chunk)
            bytes_received += len(chunk)
        
        return b''.join(chunks).decode('utf-8')

    except ValueError:
        raise ValueError(f"Trama corrupta: se esperaba una longitud de 5 dígitos, pero se recibió '{raw_msg_len.decode(errors='ignore')}'")

def _send_message(sock, service, data):
    """
    Formatea un mensaje según el protocolo y lo envía a través del socket.

    Esta es la única función para enviar mensajes, usada tanto por servicios
    como por clientes.

    Args:
        sock (socket.socket): El socket conectado al cual enviar el mensaje.
        service (str): El nombre del servicio (5 caracteres).
        data (str): El contenido de los datos a enviar.
    """
    message_data = f"{service:5s}{data}"

    # Verificación de longitud del mensaje:
    # El protocolo define un largo de 5 dígitos, por lo que el payload (Servi + Datos)
    # no puede exceder 99999 bytes.
    if len(message_data.encode('utf-8')) > 99999:
        raise ValueError(f"El mensaje es demasiado grande para enviar ({len(message_data.encode('utf-8'))} bytes). El límite del payload es 99999 bytes.")

    message = f"{len(message_data.encode('utf-8')):05d}{message_data}".encode('utf-8')
    print(f"-> [BusConnector] Enviando: {message.decode()}", flush=True)
    sock.sendall(message)


# --- INTERFACES DE ALTO NIVEL PARA SERVICIOS Y CLIENTES ---

class ServiceConnector:
    """
    Gestiona el ciclo de vida de la conexión de un servicio con el bus.
    
    Provee una interfaz de alto nivel para que la lógica de negocio de un
    servicio no tenga que lidiar con los detalles del protocolo.
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
        sinit_response = _read_payload_from_socket(self.sock)

        print(f"<- [BusConnector] Respuesta de registro: {sinit_response}", flush=True)
        
        # Valida que el bus haya confirmado el registro.
        if not sinit_response or "OK" not in sinit_response:
            raise ConnectionError("Fallo en el registro del servicio.")
        print(f"[BusConnector] Servicio '{self.service_name}' registrado.", flush=True)

    def wait_for_transaction(self):
        """
        Espera una transacción, la recibe y la parsea para la lógica de negocio.
        
        La trama que llega del bus es "ServiDatos". Esta función extrae solo "Datos".
        """
        print(f"[BusConnector] Esperando transacción para '{self.service_name}'...", flush=True)
        payload = _read_payload_from_socket(self.sock)
        
        if payload is None:
            return None
            
        print(f"<- [BusConnector] Payload recibido: {payload}", flush=True)
        # Extrae y devuelve solo los datos, que es lo que le importa al servicio.
        return payload[5:]

    def send_response(self, response_data):
        """Envía la respuesta de la lógica de negocio de vuelta al bus."""
        _send_message(self.sock, self.service_name, response_data)

    def close(self):
        """Cierra la conexión del socket si está abierta."""
        if self.sock:
            self.sock.close()
            self.sock = None
            print("[BusConnector] Conexión cerrada.", flush=True)


def transact(host, port, service_name, data_payload):
    """
    Realiza una transacción completa para un cliente.

    Esta función de alto nivel conecta, envía una solicitud, recibe y parsea
    la respuesta completa del bus (Servicio, Estado, Contenido).

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
            
            # Envía la solicitud del cliente
            _send_message(sock, service_name, data_payload)
            
            # Recibe el payload completo de la respuesta
            payload = _read_payload_from_socket(sock)
            
            if payload is None:
                return "ERROR", "NK", "Conexión cerrada por el bus."
            
            # Imprime la transacción cruda para depuración
            print(f"<- RAW Payload: {payload}", flush=True)
            
            # Parsea el payload según el formato de respuesta del bus
            r_service = payload[:5]
            r_status = payload[5:7]
            r_content = payload[7:]
            
            return r_service, r_status, r_content
        
    except Exception as e:
        print(f"[Transact] Ocurrió un error: {e}", flush=True)
        return "ERROR", "NK", str(e)