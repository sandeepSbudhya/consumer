from kafka import KafkaConsumer
import json
from .daemon_base import daemon
import datetime, sys, time

'''
Create a consumer to consume messages as a daemon process which terminates when its
default timer runs out or a EndOfStream has been received
'''
class Consumer(daemon):

    '''
    initialize the consumer with kafka server (broker), topic and client_id to help filter in the kafka server (broker)
    '''
    def __init__(self, bootstrap_servers, topic, client_id, consumerfile, pidfile):
        self.consumerfile = consumerfile
        super().__init__(pidfile=pidfile)
        self.kafkaConsumer = kafkaConsumer(
            topic,
            bootstrap_servers = bootstrap_servers,
            client_id = client_id,
            value_deserializer = lambda m: json.loads(m.decode('ascii'))
        )

    '''
    This is a method from the daemon boilerplate class overriden to consume messages
    '''
    def run(self):
        timeOfLatestMessage = None
        maxWait = datetime.timedelta(seconds=10)
        with open(self.consumerfile, 'w+') as file:
            for message in self.kafkaconsumer:
                timeOfLatestMessage = datetime.datetime.now()
                file.write(str(message.value)+'\n')
                while(true):
                    if timeOfLatestMessage + maxWait == datetime.datetime.now():
                        break
                break
        file.close()
        self.stop()
