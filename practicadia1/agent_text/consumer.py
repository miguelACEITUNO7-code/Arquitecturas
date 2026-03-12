import pika, os, time, json, random, csv, socket
from datetime import datetime

print("Esperando 30 segundos a que RabbitMQ se inicialice...")
time.sleep(30)

rabbitmq_host = os.getenv('RABBITMQ_HOST')
rabbitmq_credentials = pika.PlainCredentials(os.getenv('RABBITMQ_USERNAME'), os.getenv('RABBITMQ_PASSWORD'))

connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=rabbitmq_credentials))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

hostname = socket.gethostname()
filename = f"/data/results_{hostname}.csv"

def callback(ch, method, properties, body):
    task = json.loads(body)
    print(f" [x] Tarea recibida: {task['task_id']}")
    
    processing_time = random.randint(3, 5)
    time.sleep(processing_time)
    
    result = {
        "task_id": task["task_id"],
        "sentiment": random.choice(["positive", "negative", "neutral"]),
        "confidence": round(random.uniform(0.5, 1.0), 2),
        "timestamp": datetime.now().isoformat()
    }
    
    # LÓGICA DE GUARDADO EN CSV 
    file_exists = os.path.isfile(filename)
    
    # Abrimos el archivo en modo "append" ('a')
    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["task_id", "sentiment", "confidence", "timestamp"])
        if not file_exists:
            writer.writeheader()  # Escribe la cabecera si el archivo es nuevo
        writer.writerow(result)
    
    
    print(f" [v] Tarea procesada y guardada en CSV en {processing_time}s. Resultado: {result}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()