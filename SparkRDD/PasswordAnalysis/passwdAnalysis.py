import sys
from pyspark import SparkConf, SparkContext


def count_user_accounts(passwd_file) -> None:
    user_accounts = passwd_file \
        .count()
    
    print("The number of user accounts is: %i \n" % user_accounts)
    

def display_name_uid_gid_sorted_by_name(passwd_file_by_fields) -> None:
    
    users_by_name = passwd_file_by_fields \
        .sortBy(lambda line: line[0]) \
        .map(lambda line: [line[0], line[2], line[3]]) \
        .take(5)
    
    for (user_name, user_UID, user_GID) in users_by_name:
        print("User name: %s, UID: %s, GID: %s" % (user_name, user_UID, user_GID))


def count_users_with_bash_command(passwd_file_by_fields) -> None:
    
    users_with_bash = passwd_file_by_fields \
        .filter(lambda line: line[6] == "/bin/bash") \
        .count()
    
    print("\nThe number of users having 'bash' as command when logging is: %i" % users_with_bash)
    
    
def main(file_name: str) -> None:

    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
  
    passwd_file = spark_context.textFile(sys.argv[1])
    
    count_user_accounts(passwd_file)
    
    passwd_file_by_fields = passwd_file.map(lambda line: line.split(':'))
    
    display_name_uid_gid_sorted_by_name(passwd_file_by_fields)
          
    count_users_with_bash_command(passwd_file_by_fields)


if __name__ == "__main__":
    """
    Python program that uses Apache Spark to:
        1) Count the number of users accounts in Unix passwd file
        2) Show user name, UID and GID for the first alphabetical sorted accounts in Unix passwd file
        3) Count the number of users having "bash" command when logging
    """
    if len(sys.argv) != 2:
        print("Usage: spark-submit passwdAnalysis.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])