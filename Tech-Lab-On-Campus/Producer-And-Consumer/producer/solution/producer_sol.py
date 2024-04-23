import os 
import sys
from producer_interface import mqProducerInterface
import pika

class mqProducer(mqProducerInterface):
    def __init__(self, exchange, routing_key, body):
        # Save parameters to class variables
        self.connection = None
        self.channel = None
        self.exchange = exchange
        self.routing_key = routing_key
        self.body = body
        # Call setupRMQConnection
        self.setupRMQConnection()

    def setupRMQConnection(self):
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        
        # Establish Channel
        self.channel = self.connection.channel()

        # Create the exchange if not already present
        self.channel.exchange_declare(
            exchange= self.exchange,
            exchange_type= 'direct',
            durable= True
        )

        print("  [x] Sent Orders to RMQ")

    def publishOrder(self, str):
        # Basic Publish to Exchange
        self.channel.basic_publish(
            exchange= self.exchange,
            routing_key= self.routing_key,
            body= self.body,
        )

    def __del__(self) -> None:
        print(f"Closing RMQ connection on destruction")
        self.channel.close()
        self.connection.close()