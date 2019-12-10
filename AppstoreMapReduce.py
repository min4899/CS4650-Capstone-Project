#run with spark-submit sample.py

from pyspark.sql import SQLContext
from pyspark.sql.types import *

import re
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import col, split
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import *

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("AppstoreMapReduce")
sc = SparkContext(conf=conf)

sql = SQLContext(sc)

schema = StructType([
    StructField("url", StringType()),
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("subtitle", StringType()),
    StructField("iconURL", StringType()),
    StructField("avgUserRating", DoubleType()),
    StructField("userRatingCount", IntegerType()),
    StructField("price", DoubleType()),
    StructField("inAppPurchases", StringType()),
    StructField("developer", StringType()),
    StructField("ageRating", StringType()),
    StructField("languages", StringType()),
    StructField("size", IntegerType()),
    StructField("primaryGenre", StringType()),
    StructField("genres", StringType()),
    StructField("originalReleaseDate", StringType()),
    StructField("currentVersionDate", StringType()),
    StructField("description", StringType()),
])

print("Creating rdd from csv file")
print
df = sql.read.csv("appstore_games.csv", schema=schema, header=True)

print("Printing Schema")
df.printSchema()
print

print
print("The total number of entries is: ", df.count())
print

print("Printing the first 20 entries")
df.show()# show top 20 rows
print

rating = 4
ratingCount = 10000

print("Filter games with", rating, "+ Average User Rating and",
        ratingCount, "+ User Rating Count")
overFour = df.filter(df.avgUserRating >= rating)
df2 = overFour.filter(overFour.userRatingCount >= ratingCount)

print("The total number of entries is: ", df2.count())
print

df2.show()
print

print("Getting most popular developers")
rdd0 = df2.select('developer').rdd.map(lambda x:(x,1))
rdd1 = rdd0.reduceByKey(lambda x, y: x+y) # Reduce to get counts
rdd2 = rdd1.map(lambda x : (x[1], x[0])) # Flip key value pairs so we can sort on wordCount instead of words
rdd3 = rdd2.sortByKey(False) # Sort by descending order
print(rdd3.take(10))
print
print("Saving developer popularity data in output file")
rdd3.repartition(1).saveAsTextFile("file:///home/msoh/developers")
print

print("Getting most popular genres")
rdd5 = df2.select("genres").rdd.flatMap(lambda x: x.genres.split())
rdd5 = rdd5.filter(lambda x: x != "Games") # remove obvious tags
rdd5 = rdd5.filter(lambda x: x != "Entertainment")
rdd6 = rdd5.map(lambda x: (x,1)) # Map genre types
rdd7 = rdd6.reduceByKey(lambda x, y: x+y) # Reduce to get counts
rdd8 = rdd7.map(lambda x : (x[1], x[0])) # Flip key value pairs so we can sort on wordCount instead of words
rdd9 = rdd8.sortByKey(False) # Sort by descending order
print(rdd9.take(10))
print("Saving genre popularity data in output file")
rdd3.repartition(1).saveAsTextFile("file:///home/msoh/genres")
print

print("Getting the most common languages")
rdd5 = df2.select("languages").rdd.flatMap(lambda x: x.languages.split())
rdd6 = rdd5.map(lambda x: (x,1)) # Map
rdd7 = rdd6.reduceByKey(lambda x, y: x+y) # Reduce to get counts
rdd8 = rdd7.map(lambda x : (x[1], x[0])) # Flip key value pairs so we can sort on wordCount instead of words
rdd9 = rdd8.sortByKey(False) # Sort by descending order
print(rdd9.take(10))
print("Saving language data in output file")
rdd3.repartition(1).saveAsTextFile("file:///home/msoh/languages")
print



# GETTING SPECIFIC DATA FROM INDIVIDUAL GENRE
genreSelection = "Puzzle"

print("Showing data for genre: ", genreSelection)
print

#df3 = df2.filter(df2.genres.contains(genreSelection))

# Split genres column into new column of string arrays
df_1 = df2.withColumn(
        "genre_array",
         split(col("genres"), " ").cast("array<string>").alias("ev")
 )

# Check each string array if genre is in it, create new column with True/False
df_2 = df_1.withColumn(
         "hasGenre",
         array_contains(col("genre_array"), genreSelection)
 )

# Now dataframe only contains games with selected genre
df3 = df_2.filter(df_2.hasGenre == True)


# 1. Getting common keywords in descriptions of this genre
# function to remove stop words
remover = StopWordsRemover(inputCol="description_array", outputCol="description_clean")

# convert column of string to column of string array
df4 = df3.withColumn(
        "description_array",
        split(col("description"), " ").cast("array<string>").alias("ev")
 )
df5 = remover.transform(df4).select('description_clean') # apply stopwordremover

rdd_1 = df5.select("description_clean").rdd.flatMap(lambda x: x.description_clean) # seperate each word
rdd_2 = rdd_1.map(lambda x: re.sub(r'[^a-zA-Z\n]', '', x).lower() ) # remove punctuations
rdd_3 = rdd_2.filter(lambda x: x != "") # remove blank entries

rdd_4 = rdd_3.map(lambda x: (x,1)) # Map each word
rdd_5 = rdd_4.reduceByKey(lambda x, y: x+y) # Reduce to get counts
rdd_6 = rdd_5.map(lambda x : (x[1], x[0])) # Flip key value pairs so we can sort on wordCount instead of words
rdd_7 = rdd_6.sortByKey(False) # Sort by descending order
print("Common keywords for genre: ", genreSelection)
print(rdd_7.take(50))
print("Saving keyword count for", genreSelection, "in output file")
rdd3.repartition(1).saveAsTextFile("file:///home/msoh/keywords")
print


# 2. Getting the target age for this genre
rdd16 = df3.select("ageRating").rdd.flatMap(lambda x: x.ageRating.split())
rdd17 = rdd16.map(lambda x: (x,1)) # Map
rdd18 = rdd17.reduceByKey(lambda x, y: x+y) # Reduce to get counts
rdd19 = rdd18.map(lambda x : (x[1], x[0])) # Flip key value pairs so we can sort on wordCount instead of words
rdd20 = rdd19.sortByKey(False) # Sort by descending order
print("Target audience for genre: ", genreSelection)
print(rdd20.take(10))
print("Saving target audience for", genreSelection, "in output file")
rdd3.repartition(1).saveAsTextFile("file:///home/msoh/age")
print
