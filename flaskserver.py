import IntelligencePlaneKafkaConsumer.IPConsumer as IPConsumer
import datetime, os, time, json, logging
from tapipy.tapis import Tapis
from flask import Flask
from threading import Thread
from flask import request, jsonify
from multiprocessing import Lock

app = Flask(__name__)

server_dir = str(os.getcwd())

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler(os.path.join(server_dir, 'serverlogs/serverlog-'+str(datetime.datetime.now()))))

internal_job_id_mutex = Lock()
internal_job_id_generator = 0

#This holds all the active consumers
consumer_instances = {}

'''
responds with general info about apis
'''
@app.route("/", methods = ['GET'])
def getInfo():
    return jsonify({"message":"Send a post request to this host:port/schedulejob to request resources and this host:port/stopjob to stop a job"})

'''
api used to schedule a job. Accepts a userId and a topic to subscribe to
'''
@app.route("/schedulejob", methods=['POST'])
def scheduleJob():
    global server_dir
    try:
        topic = request.json["topic"]
        appDetails = request.json["appDetails"]
    except Exception as e:
        logger.error('wrong format of request to schedule job message')
        return (jsonify(
                {
                    "result":{
                        "message":"To schedule a job provide the topic of messages to subscribe to and details of the app to profiled/executed"
                    }
                }
            ), 400)
    
    curtimestring = str(datetime.datetime.now())
    pidfile = str(os.path.join(server_dir,'pidfiles/pidfile'+curtimestring+'.pid'))
    
    internal_job_id = None

    logger.info("pidfile: %s",pidfile)

    #check if similar app has been profiled maybe a DB call
    profiled_job_details = {}
    has_been_profiled=True

    #get job details from HARP
    profiled_job_details = {
        "name" : "mock profiled job",
        "description" : "mocked profiled app that pings kafka broker",
        "appId" : "sandeepsbudhya-ping-kafka-app",
        "appVersion" : "0.0.1"
    }

    consumerfile = str(os.path.join(server_dir, 'consumerfiles/consumerfile'+curtimestring))
    logger.info("consumerfile: %s", consumerfile)

    #get tapis access token
    t = Tapis(base_url= "https://tacc.tapis.io",
          username="sandeepsbudhya",
          password="TaccPwd123@")

    t.get_tokens()

    #submit job via tapis and generate tapis job id
    tapis_job_id = None
    try:
        job_submitted_response = t.jobs.submitJob(
            name=profiled_job_details['name'],
            appId=profiled_job_details['appId'],
            description=profiled_job_details['description'],
            appVersion=profiled_job_details['appVersion']
        )
        tapis_job_id = job_submitted_response.uuid

    except Exception as e:
        logger.error('could not submit job error %s', e)
        return (jsonify(
                {
                    "result":{
                        "message":"profiled job could not be submitted with error "+str(e)+". There could be an issue with internal tapis credentials."
                    }
                }
            ), 500)

    #generate internal job id
    with internal_job_id_mutex:
        global internal_job_id_generator
        internal_job_id_generator = internal_job_id_generator + 1
        internal_job_id = internal_job_id_generator

    #init intelligence plane kafka consumer
    try:
        ip_consumer = IPConsumer.IPConsumer(bootstrap_servers=['localhost:9092'], topic=topic, pidfile=pidfile, consumerfile=consumerfile, internal_job_id=internal_job_id)
    except Exception as e:
        logger.error(str(e)+'\nerror initializing kafka')
        return jsonify(
            {
                "result":{
                    "message":"error in initializing kafka server. Does the topic exist?"
                }
            }, 500)

    global consumer_instances
    consumer_instances[internal_job_id] = ip_consumer

    ipc_thread = Thread(target=ip_consumer.start)
    ipc_thread.daemon = True
    try:
        ipc_thread.start()
        logger.info('job %s started succesfully', str(internal_job_id))
        return (jsonify(
                {
                    "result":{
                        "message":"successfully submitted",
                        "internalJobId":str(internal_job_id),
                        "tapisJobId":str(tapis_job_id)
                    }
                }
            ), 201)

    except Exception as e:
        logger.error(str(e)+'\nerror starting job')
        del consumer_instances[internal_job_id]
        return (jsonify(
                {
                    "result":{
                        "message":"job could not be start successfully"
                    }
                }
            ), 400)

@app.route("/stopjob", methods=['POST'])
def stopjob():
    internal_job_id = request.json["internalJobId"]
    try:
        global consumer_instances
        consumer_instances[internal_job_id].stop()
        del consumer_instances[internal_job_id]
        logger.info('job %s ended successfully', str(internal_job_id))
        return (jsonify(
                {
                    "result":{
                        "message":"successfully stopped job with job id: "+str(internal_job_id)
                    }
                }
            ), 204)
    except Exception as e:
        logger.error(str(e)+'\njob may not exist')
        return (jsonify(
                {
                    "result":{
                        "message":"error stopping the job with job id: "+str(internal_job_id)+" has it already stopped?"
                    }
                }
            ), 400)

if __name__ == "__main__":
    app.run(port=8088)