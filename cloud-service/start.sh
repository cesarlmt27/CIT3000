#!/bin/sh

# Iniciar el servidor API de Rclone en segundo plano
echo "[start.sh] Iniciando servidor Rclone en segundo plano..."
rclone rcd --rc-addr=0.0.0.0:5572 --rc-user=${RCLONE_API_USER} --rc-pass=${RCLONE_API_PASS} &

# Pausa para dar tiempo a que rclone se inicie
sleep 2

# Iniciar nuestro servicio de Python en primer plano
# Este es el proceso principal del contenedor. Si este script termina, el contenedor se detiene
echo "[start.sh] Iniciando servicio de Python..."
python service.py