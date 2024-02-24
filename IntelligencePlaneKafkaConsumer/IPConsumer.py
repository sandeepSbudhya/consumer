from kafka import KafkaConsumer
import json
from .daemon_base import daemon
import datetime, sys, time
from threading import Thread


'''
Create a consumer to consume messages as a daemon process which terminates when its
default timer runs out or a EndOfStream has been received
'''
class IPConsumer(daemon):

    '''
    initialize the consumer with kafka server (broker), topic and client_id to help filter in the kafka server (broker)
    '''
    def __init__(self, bootstrap_servers, topic, pidfile, consumerfile, userId):
        self.userId = userId
        self.consumerfile = consumerfile
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        print("kafka variables: \n")
        print("bootstrap servers: ", bootstrap_servers)
        print("topic: ", topic)
        print("client: ", userId)

        super().__init__(pidfile=pidfile)
        try:
            self.perfomance_message_consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers = self.bootstrap_servers,
                value_deserializer = lambda m: json.loads(m.decode('ascii'))
            )
            self.stop_messages_consumer = KafkaConsumer(
                'stop-messages',
                bootstrap_servers = self.bootstrap_servers,
                value_deserializer = lambda m: json.loads(m.decode('ascii'))
            )
        except Exception as e:
            raise Exception('initialization failed')
        print("kafka initialized...")

    def listen_to_stop_messages(self):
        for message in self.stop_messages_consumer:
            if str(message.value["userId"]) == str(self.userId):
                break

    def listen_to_performance_messages(self, file):
        for message in self.perfomance_message_consumer:
            if str(message.value["userId"]) == str(self.userId):
                try:
                    file.write(str(message.value)+'\n')
                except Exception as e:
                    break
    '''
    This is a method from the daemon boilerplate class overriden to consume messages
    '''
    def run(self):
        t1 = Thread(target=self.listen_to_stop_messages)
        t1.start()
        file = open(self.consumerfile, "w+")
        t2 = Thread(target=self.listen_to_performance_messages, kwargs={'file':file})
        t2.start()
        while t1.is_alive():
            time.sleep(0.1)
        file.close()
        self.stop()
