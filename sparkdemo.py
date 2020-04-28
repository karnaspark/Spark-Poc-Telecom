import pyspark
from pyspark.sql import SparkSession
import configparser

#Reading the configurations from a file
config = configparser.ConfigParser()
config.read_file(open("/home/hadoop/spark-application-modes.ini"))

local_config = config['local']
yarn_config = config['yarn']

# spark = SparkSession.builder.\
#         config("spark.master",local_config['master_url']).\
#         config("spark.app.name",local_config['app_name']).\
#         getOrCreate()

spark = SparkSession.builder.\
        config("spark.master",yarn_config['master_url']).\
        config("spark.app.name",yarn_config['app_name']).\
        config('spark.driver.cores',yarn_config['driver_cores']).\
        config('spark.driver.memory',yarn_config['driver_memory']).\
        config('spark.executor.memory',yarn_config['executor_memory']).\
        config('spark.executor.cores',yarn_config['executor_cores']).\
        getOrCreate()

#Transformations here , data extract, transform and load
df = spark.createDataFrame([(1,2),(2,3),(3,4)])
print(df.show())

#Stop the application
spark.stop()
