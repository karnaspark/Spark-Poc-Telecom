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
                config("spark.executor.cores","1").\
                config("spark.executor.memory","1g").\
                getOrCreate()
        return spark                    
# Worker resources
        #         executor cores 2   memory 2g

        # 4c 8g --> 2c 2g --> e1  -->2p 
        #           2c 2g --> e2  -->2p
        # 4c 8g --> 2c 2g --> e3  -->2p
        #           2c 2g --> e4  -->2p

# In YARN, for an application an Application Master will be launched
# Application master should also run in a container.
# In YARN -> processing of applciations happen  in containers

# if n containers --> 1 container for AM and others for processing (executors)


