
Part 1: 

Execution: hadoop jar WordCount.jar WordCount.WordCount.WordCount3 books books_countInfo

This program generates the word count of all the words except the list of words in the stopWords.txt mentioned in the program.

Copy the stopWords.txt file given along with the submission to the hdfs. 

Note: move all the 6 text book files to a new directory called books and also the output will be present in the books_countInfo folder.

Part 2:

Execution: hadoop jar WordCount.jar WordCount.WordCount.WordCount2 tweets tweets_countInfo

This program generates the word count of all the hashtags i.e, the words that start with #.

Note: move all the 6 hash tag files to a new directory called tweets and also the output will be present in the tweets_countInfo folder.
