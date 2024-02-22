from kafka import kafkaConsumer
import json
from daemon_base import daemon

'''
Create a consumer to consume messages as a daemon process which terminates when its
default timer runs out or a EndOfStream has been received
'''
class Consumer(daemon):

    '''
    initialize the consumer with kafka server (broker), topic and client_id to help filter in the kafka server (broker)
    '''
    def __init__(self, bootstrap_servers, topic, client_id):
        self.kafkaConsumer = kafkaConsumer(
            topic,
            bootstrap_servers = bootstrap_servers,
            client_id = client_id,
            value_deserializer = lambda m: json.loads(m.decode('ascii'))
        )

    '''
    This is a method from the daemon boilerplate class
    '''
    def run(self):

        for message in self.kafkaconsumer:
            print(message.value)
