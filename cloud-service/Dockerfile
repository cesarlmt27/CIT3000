# Usar una imagen base de Python
FROM python:3.11-slim

# Instalar dependencias necesarias para descargar Rclone (curl, unzip)
RUN apt-get update && apt-get install -y curl unzip && rm -rf /var/lib/apt/lists/*

# Instalar Rclone usando su script oficial
RUN curl https://rclone.org/install.sh | bash

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar el archivo de dependencias e intalarlas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar los códigos del servicio y el script de inicio
COPY . .

# Dar permisos de ejecución al script de inicio
RUN chmod +x ./start.sh

# Comando que se ejecuta cuando el contenedor inicia
CMD ["./start.sh"]