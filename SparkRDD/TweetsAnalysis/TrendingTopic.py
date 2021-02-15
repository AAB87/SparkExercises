import sys

from pyspark import SparkConf, SparkContext
import nltk
from nltk.corpus import stopwords

def save_output_in_local_file(rdd, output_file_name, title):
    file = open(output_file_name, "a")
    
    file.write(title + "\n") 
    for line in rdd:
        file.write(str(line) + "\n")
    file.write("\n")
    
    file.close()
    
def obtain_most_frequent_words(rdd):
    rdd_of_pairs = rdd \
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda pair: (pair[1], pair[0])) \
        .sortByKey(False) \
        .take(10)
    
    return rdd_of_pairs

def main(file_name: str) -> None:

    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
  
    tweets_file = spark_context.textFile(sys.argv[1])
    
    common_words = []
    
    nltk.download('stopwords')
    common_words_english_language = list(stopwords.words('english'))

    common_words_tweets = ["rt", "http", "-", "2"]
    
    common_words = common_words_english_language + common_words_tweets
    
    # Words after filtering common words, but including main topic key words ("Big 12")    
    tweets_words_exluding_common_words = tweets_file\
        .map(lambda line: line.split('\t')[2])\
        .flatMap(lambda line: line.split(' '))\
        .filter(lambda word: not (word.lower().startswith(tuple(common_words))) \
                and len(word) > 0 )
    
    trending_words = obtain_most_frequent_words(tweets_words_exluding_common_words)
    
    title = "Most frequent words including main topic key words ('Big 12')"
    
    save_output_in_local_file(trending_words, "output1.txt", title)
    
    # Words after filtering common words, but excluding main topic key words ("Big 12") 
    trending_words = obtain_most_frequent_words(tweets_words_exluding_common_words \
                                                .filter(lambda word: 'big12' not in word.lower()))
    
    title = "Most frequent words excluding main topic key words ('Big 12')"
       
    save_output_in_local_file(trending_words, "output1.txt", title)
     
    spark_context.stop()    
 
if __name__ == "__main__":
    
    #Python program that uses Apache Spark to obtain the trending words from a tweets file
    
    if len(sys.argv) != 2:
        print("Usage: spark-submit TrendingTopic.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
