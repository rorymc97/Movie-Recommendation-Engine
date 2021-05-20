# !/usr/bin/env python

import sys
import os
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import udf, col, when
from functools import reduce
from pyspark.sql import DataFrame

from pyspark.sql import Row
from pyspark.sql.types import *

from IPython.display import Image
from IPython.display import display


def parseRating(line):
    """
    Parses a rating record in MovieLens format userId::movieId::rating::timestamp .
    """
    fields = line.strip().split("::")
    return int(fields[3]) % 10, (int(fields[0]), int(fields[1]), float(fields[2]))


def parseMovie(line):
    """
    Parses a movie record in MovieLens format movieId::movieTitle .
    """
    fields = line.strip().split("::")
    return int(fields[0]), fields[1]


def loadRatings(ratingsFile):
    """
    Load ratings from file.
    """
    if not isfile(ratingsFile):
        print("File %s does not exist." % ratingsFile)
        sys.exit(1)
    f = open(ratingsFile, 'r')
    ratings = filter(lambda r: r[2] > 0, [parseRating(line)[1] for line in f])
    f.close()
    if not ratings:
        print("No ratings provided.")
        sys.exit(1)
    else:
        return ratings


def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
        .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
        .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))


if __name__ == "__main__":

    # set up environment
    spark = SparkSession.builder \
        .master("local") \
        .appName("Movie Recommendation Engine") \
        .config("spark.executor.memory", "1gb") \
        .getOrCreate()

    sc = spark.sparkContext

    # load personal ratings
    myRatings = loadRatings(os.path.abspath('/home/rorymc97/CS4337/personalRatings.txt'))
    myRatingsRDD = sc.parallelize(myRatings, 1)

    # load ratings and movie titles
    movieLensHomeDir = '/home/rorymc97/CS4337/'

    # ratings is an RDD of (last digit of timestamp, (userId, movieId, rating))
    # ratings = sc.textFile(join(movieLensHomeDir, "ratings.dat")).map(parseRating)

    # movies is an RDD of (movieId, movieTitle)
    # movies = sc.textFile(join(movieLensHomeDir, "movies.dat")).map(parseMovie)

    # your code here
    # ratings is an RDD of (userId, movieId, rating, timestamp)
    ratings = sc.textFile(movieLensHomeDir + 'ratings.dat').map(lambda line: line.split("::"))

    # movies is an RDD of (movieId, movieTitle)
    movies = sc.textFile(movieLensHomeDir + 'movies.dat').map(lambda line: line.split("::"))

    # input data
    input_data = sc.textFile(movieLensHomeDir + 'personalRatings.txt').map(lambda line: line.split("::"))

    # Map the RDDs to DF
    ratings_df = ratings.map(
        lambda line: Row(userId=line[0], movieId=line[1], rating=line[2], timestamp=line[3])).toDF()
    movies_df = movies.map(lambda line: Row(movieId=line[0], movieTitle=line[1], genre=line[2])).toDF()
    input_df = input_data.map(
        lambda line: Row(userId=line[0], movieId=line[1], rating=line[2], timestamp=line[3])).toDF()

    # typecast dataframes
    ratings_df = ratings_df.withColumn("userId", ratings_df["userId"].cast(IntegerType())) \
        .withColumn("movieId", ratings_df["movieId"].cast(IntegerType())) \
        .withColumn("rating", ratings_df["rating"].cast(FloatType())) \
        .withColumn("timestamp", ratings_df["timestamp"].cast(FloatType())) \

    input_df = input_df.withColumn("userId", input_df["userId"].cast(IntegerType())) \
        .withColumn("movieId", input_df["movieId"].cast(IntegerType())) \
        .withColumn("rating", input_df["rating"].cast(FloatType())) \
        .withColumn("timestamp", input_df["timestamp"].cast(FloatType())) \

    movies_df = movies_df.withColumn("movieId", movies_df["movieId"].cast(IntegerType())) \

                # add personalRatings df onto ratings df
    union_df = input_df.union(ratings_df)

    # Split the data into train and test sets
    training_df, test_df = union_df.randomSplit([.8, .2], seed=1234)

    # Generate predictions
    als = ALS(maxIter=10, regParam=0.1, rank=4, userCol="userId", itemCol="movieId", ratingCol="rating")
    model = als.fit(training_df)
    predictions = model.transform(test_df)
    new_predictions = predictions.filter(col('prediction') != np.nan)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    # rmse = evaluator.evaluate(new_predictions)
    # print(" Root mean squared error = " + str(rmse))

    # See how predictions match actual ratings
    # predictions.show(10)

    # show predictions for one user
    # for_one_user = predictions.filter(col("userId")==0).join(movies_df, "movieId").select("userId", "movieTitle", "genre", "prediction").show(10)

    # Generate top 5 item Recommendations for each user
    user_recommends = model.recommendForAllUsers(5)

    # Show top 5 recommendations for user in format movieId's
    df = user_recommends.filter(col("userId") == 0).select("recommendations.movieId")

    # Convert recommendations to DF
    pandasDF = df.toPandas()
    pandasDF = pd.DataFrame(pandasDF, columns=['movieId', 'rating'])

    # Convert DF to a list in order to extract movieID values
    product = pandasDF['movieId'].values.tolist()
    p = product[0]

    # Iterate through movieID values and match their corresponding movieTitle
    id = p[0]
    m1 = movies_df.filter(col("movieId") == id).select("movieTitle")
    id = p[1]
    m2 = movies_df.filter(col("movieId") == id).select("movieTitle")
    id = p[2]
    m3 = movies_df.filter(col("movieId") == id).select("movieTitle")
    id = p[3]
    m4 = movies_df.filter(col("movieId") == id).select("movieTitle")
    id = p[4]
    m5 = movies_df.filter(col("movieId") == id).select("movieTitle")

    # Union all movieTitle DF's into one
    dfs = [m1, m2, m3, m4, m5]
    dfs = reduce(DataFrame.unionAll, dfs)

    # Convert movieTitle DF to pandas
    df = dfs.toPandas()
    final_df = pd.DataFrame(df, columns=['movieTitle'])

    # Convert DF to list to extartc each title
    final_list = final_df['movieTitle'].values.tolist()

    # Print each title
    print("Movies recommended for you:\n")
    j = 1
    for i in final_list:
        print(str(j) + ". " + i)
        j = j + 1

    # clean up
    sc.stop()
