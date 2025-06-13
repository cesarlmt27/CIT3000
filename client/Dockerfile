# Usar una imagen base de Python
FROM python:3.11-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar la carpeta completa del paquete común al contenedor
COPY ./common_package /app/common_package

# Usar pip para instalar el paquete local en modo 'editable'
RUN pip install -e ./common_package

# Copiar el resto del código del cliente
COPY ./client/ /app/

# Asegurarse de que el directorio de logs exista
RUN mkdir -p /app/logs

# Dar permisos de ejecución al script de inicio
RUN chmod +x /app/start-client.sh

# Comando de inicio del contenedor
CMD ["/app/start-client.sh"]