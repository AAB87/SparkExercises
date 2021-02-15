# Task: Spanish Airports
## Spark RDD

The Web site OurAirports provides open data about international airports. The data file can be downloaded from this link: airports.csv. This is a CVS file containing, among other information, the following fields:
- type: the type of the airport ("heliport", "small_airport", "large_airport", "medium_airport", etc)
- iso_country: the code of Spain is "ES".

Taking as example the ``WordCount`` program, write another program called ``SpanishAirports.py`` that reads the ``airports.csv`` file and counts the number of spanish airports for each of the four types, writing the results in a text file.