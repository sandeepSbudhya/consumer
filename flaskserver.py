import IntelligencePlaneKafkaConsumer.IPConsumer as IPConsumer
from flask import Flask
import datetime, os, time, json, logging
from threading import Thread
from flask import request, jsonify
from multiprocessing import Lock

app = Flask(__name__)

serverdir = str(os.getcwd())

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler(os.path.join(serverdir, 'serverlogs/log'+str(datetime.datetime.now()))))

mutex = Lock()
counter = 0

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
    global serverdir
    try:
        userId = request.json["userId"]
        topic = request.json["topic"]
    except Exception as e:
        logger.error('wrong format of request to schedule job message')
        return jsonify({"message":"To schedule a job provide a user id and the topic of messages to subscribe to"})
    
    curtimestring = str(datetime.datetime.now())
    pidfile = str(os.path.join(serverdir,'pidfiles/pidfile'+curtimestring+'.pid'))
    consumerfile = str(os.path.join(serverdir, 'consumerfiles/consumerfile'+curtimestring))
    jobId = None

    logger.info("pidfile: %s",pidfile)
    logger.info("consumerfile: %s", consumerfile)

    # Assume the above below lines means a job got submitted through tapis
    with mutex:
        global counter
        counter = counter + 1
        jobId = counter

    try:
        ipkc = IPConsumer.IPConsumer(bootstrap_servers=['localhost:9092'], topic=topic, pidfile=pidfile, consumerfile=consumerfile, jobId=jobId)
    except Exception as e:
        logger.error(str(e)+'\nerror initializing kafka')
        return jsonify({"message":"error in initializing kafka server"})

    global consumer_instances
    consumer_instances[jobId] = ipkc

    t = Thread(target=ipkc.start)
    t.daemon = True
    try:
        t.start()
        logger.info('job %s started succesfully', str(jobId))
        return jsonify({"message":"successfully submitted job with job id: "+str(jobId)})
    except Exception as e:
        logger.error(str(e)+'\nerror starting job')
        del consumer_instances[jobId]
        return jsonify({"message":"job could not be start successfully"})

@app.route("/stopjob", methods=['POST'])
def stopjob():
    jobId = request.json["jobId"]
    try:
        global consumer_instances
        consumer_instances[jobId].stop()
        del consumer_instances[jobId]
        logger.info('job %s ended successfully', str(jobId))
        return jsonify({"message":"successfully stopped job with job id: "+str(jobId)})
    except Exception as e:
        logger.error(str(e)+'\njob may not exist')
        return jsonify({"message":"error stopping the job with job id: "+str(jobId)+" has it already stopped?"})

if __name__ == "__main__":
    app.run(port=8088)