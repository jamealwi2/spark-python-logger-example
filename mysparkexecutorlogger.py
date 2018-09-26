import logging
import sys
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('/spark/jobs/executor.log')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s -' ' %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
