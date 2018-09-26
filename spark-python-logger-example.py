#!/usr/bin/python
from pyspark import SparkContext
import logging

# Initialize the logging for the spark driver process
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
handler = logging.FileHandler('/spark/jobs/driver.log')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s -'  ' %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)

# Initialize the spark context
sc=SparkContext.getOrCreate()

# Add the custom logger file to spark context
sc.addPyFile('maprfs:///user/mapr/spark/mysparkexecutorlogger.py')
import mysparkexecutorlogger

#Spark Action
rdd=sc.parallelize(['Hello','World','Am','Here'])
rddcount=rdd.count()
LOGGER.info('First Spark action (count of rdd) completed.')

#Sample function
def funFunction(indata):
    mysparkexecutorlogger.logger.info('Inside the fun function with some fun value %s', str(indata))
    return indata + '_addingsomefun'

# Sample RDD using the function and performing an action
rdd2=sc.parallelize(['one','two'])
rdd3=rdd2.map(lambda data:(funFunction(data[0])))

# Output the sample output
rdd3.take(2)
