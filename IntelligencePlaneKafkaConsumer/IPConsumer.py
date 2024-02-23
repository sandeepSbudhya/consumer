from kafka import KafkaConsumer
import json
from .daemon_base import daemon
import datetime, sys, time

'''
Create a consumer to consume messages as a daemon process which terminates when its
default timer runs out or a EndOfStream has been received
'''
class IPConsumer(daemon):

    '''
    initialize the consumer with kafka server (broker), topic and client_id to help filter in the kafka server (broker)
    '''
    def __init__(self, bootstrap_servers, topic, pidfile, consumerfile):
        self.consumerfile = consumerfile
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        print("kafka variables: \n")
        print(bootstrap_servers)
        print(topic)
        super().__init__(pidfile=pidfile)
        self.kafka_consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers = self.bootstrap_servers,
            value_deserializer = lambda m: json.loads(m.decode('ascii'))
        )
        print("kafka initialized...")

    '''
    This is a method from the daemon boilerplate class overriden to consume messages
    '''
    def run(self):
        with open(self.consumerfile, "w+") as file:
            for message in self.kafka_consumer:
                if 'stop listening' in str(message.value):
                    break 
                file.write(str(message.value)+'\n')
            file.close()
        self.stop()
