## Extract the basic dataframe 

import sys, re, datetime
from pyspark.sql import SparkSession, functions as f, types

spark = SparkSession.builder.appName('reddit-comment').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

schema = types.StructType([ # commented-out fields won't be read
    types.StructField('author', types.StringType(), False),
    types.StructField('created_utc', types.StringType(), False),
    types.StructField('created_date', types.StringType(), False),
    types.StructField('body', types.StringType(), False),
    types.StructField('score', types.LongType(), False),
    types.StructField('subreddit', types.StringType(), False),
    ])


def utc2date(utc):
    date = datetime.datetime.fromtimestamp(float(utc)).strftime('%Y-%m-%d %H:%M:%S')
    return date


def main():
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]

    comments = spark.read.json(in_directory, schema=schema)

    xkcd = comments.filter(
        (comments['subreddit'] == 'xkcd') & 
        (comments['author'] != ' ') &
        # (comments['score'] > 0) & 
        (comments['body'] != '[deleted]') 
        # (comments['author'] != 'autowikibot') #ignore autowikibot's auto-output comments
    )

    utc2date_udf = f.udf(utc2date, returnType=types.StringType())
    output_ = xkcd.withColumn('created_date', utc2date_udf(xkcd['created_utc']))

    final_output = output_.select(
        'author',
        'created_date',
        'body',
        'score',
        'subreddit')

    final_output.write.json(out_directory, mode ='overwrite')


if __name__=='__main__':
    main()


