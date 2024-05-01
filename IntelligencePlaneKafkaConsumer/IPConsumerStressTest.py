from kafka import KafkaConsumer
from .daemon_base import daemon
import datetime, os, logging, json, requests
from threading import Thread
from time import sleep
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler(os.path.join(str(os.getcwd()), 'IntelligencePlaneKafkaConsumer/logs/ipconsumerlogs/iplog-'+str(datetime.datetime.now()))))

'''
Create a consumer to consume messages as a daemon process which terminates when its
default timer runs out or a EndOfStream has been received
'''
class IPConsumerStressTest(daemon):

    '''
    initialize the consumer with kafka server (broker), topic and client_id to help filter in the kafka server (broker)
    bootstrap_servers: Array[String]    String:kafka broker     ip address of the kafka broker
    topic: String       the topic to subscribe to
    pidfile: String     stringified directory of file to hold the process id of the daemon
    consumerfile: String    stringified directory of file to write consumed data
    tapis_job_id: Integer      ID to filter messages from broker
    '''
    def __init__(self, bootstrap_servers, topic, pidfile, consumerfile, tapis_job_id, durationInSeconds):
        self.tapis_job_id = tapis_job_id
        self.consumerfile = consumerfile
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.durationInSeconds = durationInSeconds

        logger.info("kafka variables: ")
        logger.info("bootstrap servers: %s", str(bootstrap_servers))
        logger.info("topic: %s", str(topic))
        logger.info("job id: %s", str(self.tapis_job_id))
        logger.info("writing into %s", str(self.consumerfile))

        super().__init__(pidfile=pidfile)
        try:
            self.kafka_message_consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers = self.bootstrap_servers,
                value_deserializer = lambda m: json.loads(m.decode('ascii'))
            )
        except Exception as e:
            logger.error(str(e)+'\nerror initializing kafka topics')
            raise Exception('initialization failed')
        logger.info("kafka initialized...")

    '''
    method to launch kafka consumer to listen to messages of the topic
    '''
    def consume_messages(self):
        logger.info("consuming messages")
        for message in self.kafka_message_consumer:
            try:
                #We can process messages and discard them. File writing is just a placeholder.
                with open(self.consumerfile, "a") as file:
                    file.write(str(message.value)+'\n')
                    file.close()
            except Exception as e:
                logger.error(str(e)+'\nerror opening the file, job maybe terminated')
                break

    def keepTime(self):
        starttime = datetime.datetime.now()
        endtime = starttime + datetime.timedelta(0,self.durationInSeconds)
        logger.info("starttime: %s\nendtime: %s", str(starttime), str(endtime))
        while datetime.datetime.now() < endtime:
            sleep(0.05)

    '''
    Consume messages
    '''
    def run(self):
        logger.info('new consumer job launched')
        t1 = Thread(target=self.consume_messages)
        t1.daemon = True
        t1.start()
        self.keepTime()
        res = requests.post("http://localhost:8088/stopjob", headers = {'Content-Type':'application/json'},
                            json = {"tapisJobId":str(self.tapis_job_id)}, timeout=2.50)
        logger.info(res.text)