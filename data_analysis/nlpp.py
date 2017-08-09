## Natural language preprocessing

import nltk, sys, re, string, datetime
import numpy as np 
import pandas as pd
from nltk.corpus import stopwords
from nltk.corpus import wordnet
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession, functions as f, types
from scipy import stats

spark = SparkSession.builder.appName('reddit-comment-polar').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

schema = types.StructType([ # commented-out fields won't be read
	#types.StructField('author', types.StringType(), False),
	#types.StructField('created_utc', types.StringType(), False),
	types.StructField('created_date', types.StringType(), False),
	types.StructField('polarity', types.StringType(), False),
	types.StructField('body', types.StringType(), False),
	types.StructField('refined_body', types.StringType(), False),
	types.StructField('hour', types.StringType(), False),
	types.StructField('polarity_score', types.StringType(), False)


	#types.StructField('score', types.LongType(), False),
	#types.StructField('subreddit', types.StringType(), False),
	])


# Remove low_value data 
def regex_filtering(data):

	#remove URL
	url = re.compile('([--:\w?@%&+~#=]*\.[a-z]{2,4}\/{0,2})((?:[?&](?:\w+)=(?:\w+))+|[--:\w?@%&+~#=]+)?')
	data = url.sub(' ', data)

	#remove number
	number = re.compile('(\\d+)')
	data = number.sub(' ', data)

	#remove punctuation
	punctuation = re.compile('[%s]' % re.escape(string.punctuation))	
	data = punctuation.sub(' ', data)

	return data.lower() #convert to lowercase
	

# sentiment analysis
def sentiment_analysis(sentence):
	sia = SentimentIntensityAnalyzer()
	polarity_dict = sia.polarity_scores(sentence) 
	score = pos, neu, neg = polarity_dict['pos'], polarity_dict['neu'], polarity_dict['neg']
	if max(score) == neu:
		return ' '
	elif max(score) == pos:
		return 'POG: ' + str(pos)
	elif max(score) == neg:
		return 'NEG: ' + str(neg)


def polarity_label(score):
	if score.startswith('POG:'):
		return 'POG'
	elif score.startswith('NEG:'):
		return 'NEG'

def extract_hour(date):
	dt = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
	return str(dt.hour)+':00'

def main():
	in_directory = sys.argv[1]
	out_directory = sys.argv[2]

	comments = spark.read.json(in_directory, schema=schema)

	#comment filter
	regex_filtering_udf = f.udf(regex_filtering, types.StringType())
	body = comments.withColumn('refined_body', regex_filtering_udf(comments['body']))

	# sentiment_analysis
	sentiment_analysis_udf = f.udf(sentiment_analysis, types.StringType())
	polar_score = body.withColumn('polarity_score', sentiment_analysis_udf(body['refined_body']))

	nonull = polar_score.filter(polar_score['polarity_score'] != ' ')

	# polarity label:
	polarity_label_udf = f.udf(polarity_label, types.StringType())
	polar_label = nonull.withColumn('polarity', polarity_label_udf(nonull['polarity_score']))

	extract_hour_udf = f.udf(extract_hour, types.StringType())
	hour = polar_label.withColumn('hour', extract_hour_udf(polar_label['created_date']))

	count = hour.groupBy(['hour', 'polarity']).agg(f.count('polarity').alias('count'))
	

	output = count.select(
		'hour',
		'polarity',
		'count'
		)

	output.write.json(out_directory, mode ='overwrite')

if __name__=='__main__':
	main()












# # Tokenlize, Stem Remove stopwords, Lemmatize		
# def word_tokenize(data):

# 	filter_ = re.compile(r'[\w]+') 
	
# 	stopwords_book = nltk.corpus.stopwords.words('english')
	
# 	porter = nltk.PorterStemmer()#PorterStemmer

# 	stem_list = []

# 	for word in nltk.regexp_tokenize(data, filter_): #tokenlize
		
# 		newWord = porter.stem(word) # stemming
		
# 		if newWord in stopwords_book: # remove stop words
# 			continue
		
# 		tokenSynsets = wordnet.synsets(newWord)
# 		#assume choosing the first thesaurus in the wordnet 
# 		stem_list.append(newWord if tokenSynsets == [] else tokenSynsets[0].lemma_names()[0]) #lemmatize
	
# 	return stem_list


























