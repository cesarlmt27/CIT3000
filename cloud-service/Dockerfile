# Usar una imagen base de Python
FROM python:3.11-slim

# Instalar dependencias necesarias para descargar Rclone (curl, unzip)
RUN apt-get update && apt-get install -y curl unzip && rm -rf /var/lib/apt/lists/*

# Instalar Rclone usando su script oficial
RUN curl https://rclone.org/install.sh | bash

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar la carpeta completa del paquete común al contenedor
COPY ./common_package /app/common_package

# Usar pip para instalar el paquete local en modo 'editable'
RUN pip install -e ./common_package

# Copiar el archivo de dependencias e intalarlas
COPY ./cloud-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar los códigos del servicio y el script de inicio
COPY ./cloud-service/ .

# Dar permisos de ejecución al script de inicio
RUN chmod +x ./start.sh

# Comando de inicio del contenedor
CMD ["./start.sh"]