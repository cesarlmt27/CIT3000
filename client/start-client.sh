#!/bin/sh

# Iniciar el scheduler en segundo plano
echo "[start-client.sh] Iniciando scheduler.py en segundo plano..."
python scheduler.py > /app/logs/scheduler_stdout.log 2> /app/logs/scheduler_stderr.log &

# Iniciar el cliente interactivo en primer plano
echo "[start-client.sh] Iniciando main_client.py en primer plano..."
python main_client.py