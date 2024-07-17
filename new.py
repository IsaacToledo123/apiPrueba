import pika
import json
import socketio

RABBITMQ_HOST = 'amqps://vumnphwp:04G37mBLNQfL_i6oM1cfMffWzwOOJifD@shrimp.rmq.cloudamqp.com/vumnphwp'

QUEUE_NAMES = {
    'nivel_agua': 'nivelAgua',
    'ph': 'ph',
    'nivelFertilizante':'nivelFertilizante',
    'flujo_agua': 'flujoAgua'
}

# Initialize SocketIO client
sio = socketio.Client()

@sio.event
def connect():
    print('WebSocket connected')

@sio.event
def disconnect():
    print('WebSocket disconnected')

def process_message(ch, method, properties, body):
    data = json.loads(body)
    print(f" [x] Received {data} from {method.routing_key}")
    # Add your processing logic here based on the queue name and message content
    if method.routing_key == QUEUE_NAMES['nivel_agua']:
        handle_nivel_agua(data)
    elif method.routing_key == QUEUE_NAMES['ph']:
        handle_ph(data)
    elif method.routing_key == QUEUE_NAMES['flujo_agua']:
        handle_flujo_agua(data)

    # Emit data to WebSocket server
    sio.emit(method.routing_key, data)

def handle_nivel_agua(data):
    print("Processing nivel_agua data:", data)
    # Implement your processing logic here

def handle_ph(data):
    print("Processing ph data:", data)
    # Implement your processing logic here

def handle_flujo_agua(data):
    print("Processing flujo_agua data:", data)
    # Implement your processing logic here

def start_consuming():
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
    channel = connection.channel()

    for queue_name in QUEUE_NAMES.values():
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_consume(queue=queue_name, on_message_callback=process_message, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    # Connect to WebSocket server
    sio.connect('http://localhost:3005')
    start_consuming()
