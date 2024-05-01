"""Generic linux daemon base class for python 3.x."""

import sys, os, time, atexit, signal, logging, datetime
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler(os.path.join(str(os.getcwd()), 'IntelligencePlaneKafkaConsumer/logs/daemonlogs/daemonlog-'+str(datetime.datetime.now()))))

class daemon:
	"""A generic daemon class.

	Usage: subclass the daemon class and override the run() method."""

	def __init__(self, pidfile):
		self.pidfile = pidfile
	
	def daemonize(self):
		"""Deamonize class. UNIX double fork mechanism."""
		logger.info('initializing daemon...')
		try: 
			pid = os.fork() 
			if pid > 0:
				# exit first parent
				sys.exit(0) 
		except OSError as err:
			logger.error('fork #1 failed')
			sys.exit(1)
	
		# decouple from parent environment
		os.chdir('/') 
		os.setsid() 
		os.umask(0) 
	
		# do second fork
		try: 
			pid = os.fork() 
			if pid > 0:
				# exit from second parent
				sys.exit(0)
				
		except OSError as err: 
			logger.error('fork #2 failed')
			sys.exit(1) 
	
		# write pidfile
		atexit.register(self.delpid)

		pid = str(os.getpid())
		logger.info('daemon pid: %s', pid)
		with open(self.pidfile,'w+') as f:
			f.write(pid + '\n')
	
	def delpid(self):
		os.remove(self.pidfile)

	def start(self):
		"""Start the daemon."""
		logger.info('starting daemon...')
		# Check for a pidfile to see if the daemon already runs
		try:
			with open(self.pidfile,'r') as pf:
				pid = int(pf.read().strip())

		except IOError:
			pid = None
	
		if pid:
			message = "pidfile {0} already exist. " + \
					"Daemon already running?\n"
			logger.error(message)
			sys.exit(1)
		
		# Start the daemon
		self.daemonize()
		self.run()

	def stop(self):
		logger.info('stopping daemon...')
		"""Stop the daemon."""

		# Get the pid from the pidfile
		try:
			with open(self.pidfile,'r') as pf:
				pid = int(pf.read().strip())
		except IOError:
			pid = None
	
		if not pid:
			message = "pidfile {0} does not exist. " + \
					"Daemon not running?\n"
			logger.error(message)
			return # not an error in a restart

		# Try killing the daemon process	
		try:
			while 1:
				logger.info('trying to kill process with pid %s',str(pid))
				os.kill(pid, signal.SIGTERM)
				time.sleep(0.1)
		except OSError as err:
			e = str(err.args)
			if e.find("No such process") > 0:
				if os.path.exists(self.pidfile):
					os.remove(self.pidfile)
					logger.info('daemon stopped successfully')
			else:
				logger.error(str(err.args))
				sys.exit(1)

	def run(self):
		"""You should override this method when you subclass Daemon.
		
		It will be called after the process has been daemonized by 
		start() or restart()."""

