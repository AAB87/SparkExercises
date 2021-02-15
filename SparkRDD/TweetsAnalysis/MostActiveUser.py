import sys

from pyspark import SparkConf, SparkContext

def save_output_in_local_file(rdd, output_file_name):
    file = open(output_file_name, "w")
    
    file.write("Most active user is: %s, with %i tweets. \n\n" %(rdd[0][1], rdd[0][0]))
    
    file.write("Top 10 of most active users: \n")
    for (number_of_tweets, user) in rdd:
        file.write("User: %s, Number of tweets: %i\n" % (user, number_of_tweets))
    
    file.close()

def main(file_name: str) -> None:

    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
  
    tweets_file = spark_context.textFile(sys.argv[1])
    
    most_active_users = tweets_file\
        .map(lambda line: line.split('\t')[1]) \
        .map(lambda user: (user, 1))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda pair: (pair[1], pair[0])) \
        .sortByKey(False) \
        .take(10)
        
    save_output_in_local_file(most_active_users, "output3.txt")
    
    spark_context.stop()    
 
if __name__ == "__main__":
    
    #Python program that uses Apache Spark to obtain most active user in tweets file
    
    if len(sys.argv) != 2:
        print("Usage: spark-submit MostActiveUser.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])