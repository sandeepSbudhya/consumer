from flask import Flask

app = Flask(__name__)

@app.route("/schedulejob", methods=['POST'])
def scheduleJob():
    return "<!doctype html><title>Feature not available</title><h1>This feature still needs some work before being available</h1>"

