import pika
import os
import time
import json
import uuid
import random

print("Esperando 30 segundos a que RabbitMQ se inicialice...")
time.sleep(30)  # Wait for RabbitMQ container to initialize

# Obtenemos las credenciales y el host desde las variables de entorno
rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq') # 'rabbitmq' por defecto
rabbitmq_username = os.getenv('RABBITMQ_USERNAME', 'guest')
rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')

credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)

# Conexión
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
)
channel = connection.channel()

# Declaramos la cola (durable=True ayuda a no perder tareas si el broker se reinicia)
channel.queue_declare(queue='task_queue', durable=True)

# Textos de ejemplo para simular el contenido de las tareas
contents = [
    "This product is amazing!",
    "I really hated the service.",
    "It's an okay experience, nothing special.",
    "Absolutely fantastic quality."
]

while True:
    # Construimos la tarea con el formato del enunciado
    task = {
        "task_id": str(uuid.uuid4()),
        "type": "text_analysis",
        "content": random.choice(contents)
    }
    
    # Enviamos la tarea a la cola convirtiendo el diccionario a JSON string
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=json.dumps(task)
    )
    
    print(f" [x] Sent {task}")
    
    # Pausa de 1 segundo exacto
    time.sleep(1)

connection.close()