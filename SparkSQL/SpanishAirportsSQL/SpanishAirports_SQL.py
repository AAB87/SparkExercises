from pyspark.sql import SparkSession


def save_in_local_file(data_frame, output_file_name):
    file = open(output_file_name, "w")
    file.write("Number of Spanish airports by type: " + "\n") 
    for line in data_frame.collect():
        file.write(str(line) + "\n")
    file.close()
  
    
def main() -> None:
    
    spark_session = SparkSession \
        .builder \
        .getOrCreate()
    
    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
    
    airports_data_frame = spark_session\
        .read\
        .format("csv")\
        .option("header", "true")\
        .load("airports.csv")
    
    airports_data_frame.printSchema()
    
    airports_data_frame.show(2)
    
    spanish_airports_data_frame = airports_data_frame\
        .filter(airports_data_frame["iso_country"]=="ES")\
        .groupBy("type")\
        .count()\
        .sort("count", ascending=False)
    
    spanish_airports_data_frame.show()
    
    save_in_local_file(spanish_airports_data_frame, "output.txt")
        
    
if __name__ == "__main__":
    """
    Python program that uses Apache Spark to count Spanish airports by type, using DataFrames
    """
    main()
