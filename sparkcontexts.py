from pyspark import SparkContext
from pyspark.sql import SparkSession

def sparksession_local():
        """
        input: none
        Output: Spark driver /spark session object
        """
        spark = SparkSession.builder.\
                config("spark.master","local[2]").\
                config("spark.app.name","localapp").\
                config("spark.driver.memory","2g").\
                getOrCreate()
        return spark                    

#We can submit a spark appl to yarn/any cluster  either in client mode or cluster mode
#driver configurations are required only in cluster mode,not in client mode
#In client mode, the driver program will be launched in client machine and in cluster mode, driver program will be launched in one of the workers
# Executors will be launched always on worker nodes, responsible for processing the data
# Driver will be launched depending upon the mode , 
#The mode recommended for production jobs is cluster mode

def sparksession_yarn():
        """
        input: none
        Output 
        param: spark: Spark driver /spark session object
        """

        spark = SparkSession.builder.\
                config("spark.master","yarn").\
                config("spark.app.name","yarnapp").\
                config("spark.driver.memory","2g").\
                config("spark.driver.cores","2").\
                config("spark.executor.cores","2").\
                config("spark.executor.memory","2g").\
                getOrCreate()
        return spark                    

