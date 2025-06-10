# Usar una imagen base de Python
FROM python:3.11-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar la carpeta completa del paquete común al contenedor
COPY ./common_package /app/common_package

# Usar pip para instalar el paquete local en modo 'editable'
RUN pip install -e ./common_package

# Copiar el archivo de dependencias e intalarlas
COPY ./backup-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código
COPY ./backup-service/ .

# Comando de inicio del contenedor
CMD ["python", "service.py"]