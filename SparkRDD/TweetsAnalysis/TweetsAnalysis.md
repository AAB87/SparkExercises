# Task: Tweets analysis
## Spark RDD

Taking as data source the file ``tweets.txt``, which contains tweets written during a game of the USA universitary basketball league, write the following programs:
- ``TrendingTopic.py``: search for the 10 most repeated words in the matter field of the tweets. The output of the program must be a file containing the words and their frequency, ordered by frequencey. Note that common words (e.g. "RT", "http", ...) should be filtered.
- ``MostActiveUser.py``: find the user that has written the largest amount of tweets. The program will print in the screen the name of the user and the number of tweets.
- ``LongestTweet.py``: find the longest tweet emitted by any user. The program will print in the screen the user name, the length of the tweet, and the time and date of the tweet.