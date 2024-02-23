import IntelligencePlaneKafkaConsumer.Consumer as Consumer
from flask import Flask
import datetime, os
from threading import Thread
app = Flask(__name__)

@app.route("/", methods = ['GET'])
def getInfo():
    return "<!doctype html><title>Schedule a job</title><h1>Send a post request to this host/schedulejob with job details to get an estimation of time</h1>"

@app.route("/schedulejob", methods=['POST'])
def scheduleJob():
    curtimestring = str(datetime.datetime.now())
    serverdir = str(os.getcwd())
    pidfile = os.path.join(serverdir,'pidfiles/pidfile'+curtimestring+'.pid')
    consumerfile = os.path.join(serverdir, 'consumerfiles/consumerfile'+curtimestring)
    kc = Consumer.Consumer('localhost:9092','performance-messages','1', pidfile=pidfile, consumerfile=consumerfile)
    t = Thread(target=kc.start)
    t.daemon = True
    t.start()
    return "<!doctype html><title>scheduled</title><h1>Your job is scheduled</h1>"

if __name__ == "__main__":
    app.run(port=8088)