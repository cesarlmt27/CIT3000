# Usar una imagen base de Python
FROM python:3.11-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el archivo de dependencias e intalarlas
# COPY requirements.txt .
# RUN pip install -r requirements.txt

# Copiar el resto del código
COPY . .

# Comando de inicio del contenedor
CMD ["python", "client.py"]