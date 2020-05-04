import sparkcontexts
from sparkcontexts import sparksession_local,sparksession_yarn
import datetime
# from datetime import datetime,time
import time
from pyspark.sql import SparkSession

if __name__  ==  "__main__":

    spark = SparkSession.builder.appName("Sparkapp").getOrCreate()
    
    # ETL
    rdd = spark.sparkContext.parallelize(range(1,1000))
    rdd2 = rdd.repartition(8)
    
    for i in range(10):
        time.sleep(30)
        print(rdd2.collect())


    spark.stop()
