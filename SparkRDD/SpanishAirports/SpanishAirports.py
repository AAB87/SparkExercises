import sys

from pyspark import SparkConf, SparkContext

def save_in_local_file(rdd_of_pairs, output_file_name):
    file = open(output_file_name, "w")
    file.write("Number of Spanish airports by type: " + "\n") 
    for line in rdd_of_pairs:
        file.write(str(line) + "\n")
    file.close()
    
def main(file_name: str) -> None:

    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    airports_file = spark_context.textFile(sys.argv[1])
    
    header = airports_file.first()
    
    spanish_airports_by_type = airports_file \
        .filter(lambda line: header not in line) \
        .map(lambda line: line.split(',')) \
        .filter(lambda line: line[8] == '"ES"') \
        .map(lambda line: (line[2], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()

    for (airport_type, count) in spanish_airports_by_type:
        print("%s: %i" % (airport_type, count))
    
    save_in_local_file(spanish_airports_by_type, "output.txt")
        
    spark_context.stop()
    
if __name__ == "__main__":
    """
    Python program that uses Apache Spark to count Spanish airports by type
    """
    if len(sys.argv) != 2:
        print("Usage: spark-submit SpanishAirports.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])