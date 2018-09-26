import logging
import sys
import os

# Initializing the logger
logger = logging.getLogger(__name__)

# Setting the log level to 'DEBUG'
logger.setLevel(logging.DEBUG)

# Setting the executor log file location 
handler = logging.FileHandler('/spark/jobs/executor.log')
handler.setLevel(logging.DEBUG)

# Setting the log format
formatter = logging.Formatter('%(asctime)s -' ' %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
