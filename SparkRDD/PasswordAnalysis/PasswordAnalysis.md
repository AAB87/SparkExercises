# Task: Password analysis
## Spark RDD

All the UNIX-like operating systems (such as Linux and macOS) contains a file name passwd which is located in the ``/etc`` directory. This /etc/passwd file contains a line per user account in the system, such as this one:

``yarn:*:345:535:Hadoop Yarn:/var/lib/hadoop-yarn:/bin/bash``

Each line contains the following fields separated by the ':' character:
- user name (a string)
- a character (* or x, depending on the system)
- the user identifier or UID (an integer value)
- the group identifier or GID (an integer value)
- the full user name (a string)
- the home directory of the user (a string)
- the command to run when the user logs in the system (a string)

Write a spark program called ``passwdAnalysis.py`` that, taking as argument the ``/etc/passwd`` file, do the following: 
- print the number of users having an account in the system (e.g. "The number of user accounts is: x")
- sort the users by user name and print the five first users, indicating only their name and their UID and GID.
- print the numbers of users having the command ``/bin/bash`` as last field (e.g. "The number of users having bash as command when logging is: x")