import sparkcontexts
from sparkcontexts import sparksession_local,sparksession_yarn
import datetime
# from datetime import datetime,time
import time

if __name__  ==  "__main__":

    spark = sparksession_yarn()
    
    # ETL
    rdd = spark.sparkContext.parallelize(range(1,100))
    time.sleep(120)
    print(rdd.collect())


    spark.stop()




