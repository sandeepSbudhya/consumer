import IntelligencePlaneKafkaConsumer.IPConsumer as IPConsumer
import IntelligencePlaneKafkaConsumer.LaunchProfiler as LaunchProfiler
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

tapis_job_mutex = Lock()
tapis_job_id_generator = 0

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
        return jsonify({"message":"To schedule a job provide a user id and the topic of messages to subscribe to"})
    
    curtimestring = str(datetime.datetime.now())
    pidfile = str(os.path.join(server_dir,'pidfiles/pidfile'+curtimestring+'.pid'))
    
    job_id = None

    logger.info("pidfile: %s",pidfile)

    #check if similar app has been profiled maybe a DB call
    has_been_profiled=True
    profiled_app_details = {
        "name" : "dummy profiled app",
        "description" : "dummy profiled app description",
        "appId" : "sandeepsbudhya-test-app",
        "appVersion" : "0.1"
    }

    # #submit job through tapis
    consumerfile = str(os.path.join(server_dir, 'consumerfiles/consumerfile'+curtimestring))
    logger.info("consumerfile: %s", consumerfile)
    t = Tapis(base_url= "https://tacc.tapis.io",
          username="sandeepsbudhya",
          password="TaccPwd123@")

    t.get_tokens()
    tjid = None
    try:
        job_submitted_response = t.jobs.submitJob(   name=profiled_app_details['name'], appId=profiled_app_details['appId'],
                        description=profiled_app_details['description'], appVersion=profiled_app_details['appVersion'])
        tjid = job_submitted_response.uuid
    except Exception as e:
        logger.error('could not submit job error %s', e)
        return jsonify({"message":"profiled job could not be submitted with error "+str(e)})

    # Assume the above below lines means a job got submitted through tapis
    with tapis_job_mutex:
        global tapis_job_id_generator
        tapis_job_id_generator = tapis_job_id_generator + 1
        job_id = tapis_job_id_generator

    #init intelligence plane kafka consumer
    try:
        ip_consumer = IPConsumer.IPConsumer(bootstrap_servers=['localhost:9092'], topic=topic, pidfile=pidfile, consumerfile=consumerfile, job_id=job_id)
    except Exception as e:
        logger.error(str(e)+'\nerror initializing kafka')
        return jsonify({"message":"error in initializing kafka server"})

    global consumer_instances
    consumer_instances[job_id] = ip_consumer

    t = Thread(target=ip_consumer.start)
    t.daemon = True
    try:
        t.start()
        logger.info('job %s started succesfully', str(job_id))
        return jsonify({"message":"successfully submitted job with job id: "+str(job_id)})
    except Exception as e:
        logger.error(str(e)+'\nerror starting job')
        del consumer_instances[job_id]
        return jsonify({"message":"job could not be start successfully"})

@app.route("/stopjob", methods=['POST'])
def stopjob():
    job_id = request.json["jobId"]
    try:
        global consumer_instances
        consumer_instances[job_id].stop()
        del consumer_instances[job_id]
        logger.info('job %s ended successfully', str(job_id))
        return jsonify({"message":"successfully stopped job with job id: "+str(job_id)})
    except Exception as e:
        logger.error(str(e)+'\njob may not exist')
        return jsonify({"message":"error stopping the job with job id: "+str(job_id)+" has it already stopped?"})

if __name__ == "__main__":
    app.run(port=8088)