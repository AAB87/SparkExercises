import sys

from pyspark import SparkConf, SparkContext

def save_output_in_local_file(rdd, output_file_name):
    file = open(output_file_name, "w")
    file.write("Information about the longest tweets:" + "\n") 
    
    for (user, tweet_length, date_time) in rdd:
        file.write("User: %s, Tweet length: %s, Date and time: %s" % (user, str(tweet_length), str(date_time)) + "\n")

    file.close()

def main(file_name: str) -> None:

    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
  
    tweets_file = spark_context.textFile(sys.argv[1])
    
    tweet_lengths = tweets_file\
        .map(lambda line: line.split('\t')[2]) \
        .map(lambda tweet: len(tweet))\
        .collect()
    
    longest_tweet_length = max(tweet_lengths)
    
    info_longest_tweets = tweets_file \
        .map(lambda line: line.split('\t'))\
        .filter(lambda line: len(line[2]) == longest_tweet_length) \
        .map(lambda line: [line[1], len(line[2]), line[3]]) \
        .collect()
    
    save_output_in_local_file(info_longest_tweets, "output2.txt")
    
    spark_context.stop()    
 
if __name__ == "__main__":
    
    #Python program that uses Apache Spark to obtain most active user in tweets file
    
    if len(sys.argv) != 2:
        print("Usage: spark-submit LongestTweet.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])