# import IntelligencePlaneKafkaConsumer.IPConsumer as IPConsumer
import IntelligencePlaneKafkaConsumer.IPConsumerStressTest as IPConsumerStressTest
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

#This holds all the active consumers
consumer_instances = {}

#This is the mutex for accessing the consumer instances
consumerInstancesMutex = Lock()

#This is the mutex for stress test job id
stressTestIdMutex = Lock()

stressTestId = 0

'''
responds with general info about apis
'''
@app.route("/", methods = ['GET'])
def getInfo():
    return jsonify({"message":"Send a post request to this host:port/schedulejob to request resources and this host:port/stopjob to stop a job"})

@app.route("/stopjob", methods=['POST'])
def stopjob():
    tapis_job_id = str(request.json["tapisJobId"])
    try:
        global consumer_instances
        consumer_instances[tapis_job_id].stop()
        with consumerInstancesMutex:
            del consumer_instances[tapis_job_id]
        logger.info('job %s ended successfully', tapis_job_id)
        return (jsonify(
                {
                    "result":{
                        "message":"successfully stopped job with job id: "+tapis_job_id
                    }
                }
            ), 200)
    except Exception as e:
        logger.error(str(e)+' job may not exist')
        return (jsonify(
                {
                    "result":{
                        "message":"error stopping the job with job id: "+tapis_job_id+" has it already stopped?"
                    }
                }
            ), 400)

@app.route("/launchstresstestjob", methods=['POST'])
def launchstresstestjob():
    durationInSeconds = 180
    try:
        durationInSeconds=request.json["durationInSeconds"]
    except Exception as e:
        return (jsonify(
                {
                    "result":{
                        "message":"could not launch stress test job. Check format and try again"
                    }
                }
            ), 400)

    global server_dir
    curtimestring = str(datetime.datetime.now())
    pidfile = str(os.path.join(server_dir,'pidfiles/pidfile'+curtimestring+'.pid'))
    logger.info("pidfile: %s",pidfile)
    consumerfile = str(os.path.join(server_dir, 'consumerfiles/consumerfile'+curtimestring))
    logger.info("consumerfile: %s", consumerfile)

    localStressTestId = None
    #generate stress test id
    with stressTestIdMutex:
        global stressTestId
        stressTestId = stressTestId + 1
        localStressTestId = stressTestId

    #init intelligence plane kafka consumer
    try:
        ip_consumer =   IPConsumerStressTest.IPConsumerStressTest(
                            bootstrap_servers=['<ENTER YOUR KAFKA BROKER URL>:9092'],
                            topic='utilization-messages',
                            pidfile=pidfile,
                            consumerfile=consumerfile,
                            tapis_job_id=str(localStressTestId),
                            durationInSeconds=durationInSeconds
                        )
    except Exception as e:
        logger.error(str(e)+'\nerror initializing kafka')
        return jsonify(
            {
                "result":{
                    "message":"error in initializing kafka server. Does the topic exist?"
                }
            }, 500)

    global consumer_instances
    with consumerInstancesMutex:
        consumer_instances[str(localStressTestId)] = ip_consumer

    ipc_thread = Thread(target=ip_consumer.start)
    ipc_thread.daemon = True
    try:
        ipc_thread.start()
        logger.info('job %s started succesfully', str(localStressTestId))
        return (jsonify(
                {
                    "result":{
                        "message":"successfully submitted",
                        "tapisJobId":str(localStressTestId),
                        "accessToken" : "N/A"
                    }
                }
            ), 201)
    except Exception as e:
        return (jsonify(
                {
                    "result":{
                        "message":"could not start kafka consumer",
                        "tapisJobId":"N/A",
                        "accessToken" : "N/A"
                    }
                }
            ), 500)

@app.route("/stopallstresstestjobs", methods=['POST'])
def stopallstresstestjobs():
    global consumer_instances
    while consumer_instances:
        with consumerInstancesMutex:
            for ipkc in consumer_instances:
                try:
                    consumer_instances[ipkc].stop()
                    logger.info('stress test job %s ended successfully', str(ipkc))

                except Exception as e:
                    logger.info('stress test job %s might have already ended', str(ipkc))
            consumer_instances.clear()
        time.sleep(0.05)

    return (jsonify({
        "result":{
            "message":"Stopped all stress test jobs"
        }
    }), 200)

    return (jsonify({
            "result":{
                "message":"successfully stopped all stress test jobs"
            }
        }), 200)

if __name__ == "__main__":
    app.run(port=8088)