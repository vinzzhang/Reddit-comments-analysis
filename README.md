# data_analysis
data_analysis about the relation between the polarity of reddit comments and comments' created time to check whether people tend to made postive/negtive comment in day or night.

firstly, i used rediit_generate.py to get raw data

then, use nlpp.py to refined data

at last, analysis data.py to analysis

(the output.json is the data that will be used in analysing)


running process:

spark-submit reddit_generate.py reddit-2 output
spark-submit nlpp.py output test
cat test/part-* | less 
manully copy the the whole output into a Json file, named output.json
python3 analysis.py 


