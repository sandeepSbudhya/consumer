
# Intelligence Plane Flask Kafka message system

Flask + Kafka project to demonstrate intelligence plane communication.

## Setup

 1. Clone the repository:<br/> `git clone https://github.com/sandeepSbudhya/consumer.git`
 2. This project requires **Python** to install dependencies. <br/>Check if python is installed with :<br/>`python -V` 
 3. Create a virtual environment to run the app. Here is a sample command using pipenv:<br/> From the **root** directory:<br/>`pipenv shell`
 4. After activating the environment, from **root** directory run:<br/>`pip install -r requirements.txt`<br/> to install all the required dependencies in the activated environment. 
 5. Change the server to your public **kafka** server IP Address in **line 67** of the file *root*/flaskserver.py
 6. From the **root** directory run:<br/>`python flaskserver.py`<br/>This will **run** the application locally.
## APIs available
### Ports
Port exposed when the flask application is running:


http://localhost:5000 This is the port that the flask server application's controllers receives requests on.
### Endpoints
http://localhost:5000/schedulejob<br/>

 1. Send a **POST** message to  this endpoint  with json body:<br/>{<br/>&emsp;"topic":"performance-messages" or "progress-messages",<br/>&emsp;"appDetails":{<br/>&emsp;&emsp;"id":"my-tapis-app",<br/>
&emsp;&emsp;"version":"0.1",<br/>
&emsp;&emsp;"description":"My sample application",<br/>
&emsp;&emsp;"runtime":"DOCKER",<br/>
&emsp;&emsp;"containerImage":"docker.io/hello-world:latest",<br/>
&emsp;&emsp;"jobType":"FORK",<br/>
&emsp;&emsp;"jobAttributes":  {<br/>
&emsp;&emsp;&emsp;"description":  "some job description"<br/>
&emsp;&emsp;}<br/>&emsp;}<br/>}<br/><br/>to listen to messages from the job for that particular topic. The response will be a **success** message as well as an **access token** and a **tapis job ID**. This starts a daemon process which connects to the **kafka** server and filters the consumed messages by the appropriate **tapis job ID**.<br/> <br/>These filtered messages are written to file and are available in:<br/><br/> *root*/consumerfiles/\<timestamped consumer file\>

 2. Send **POST** message to the following url with json body as shown along with the **correct** **tapis job ID** to kill the listener and stop the daemon process.<br/>http://localhost:5000/stopjob<br/>{<br/>
&emsp;"tapisJobId"  :  "\<the tapis job id from the response after scheduling the job\>",<br/>}<br/><br/>


