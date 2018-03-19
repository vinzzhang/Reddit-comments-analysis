import sys
import numpy as np
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
plt.switch_backend('agg')
from scipy import stats
from sklearn.metrics import r2_score
from datetime import date



def str_2_num(s):
	return int(s.replace(':00',''))


def main():

	rd = pd.read_json("output.json", lines=True)

	#filter data
	rd['hour'] = rd['hour'].apply(lambda x: str_2_num(x))
	
	day = rd[(rd['hour'] >= 7) & (rd['hour'] <= 18)]
	night= rd[(rd['hour'] < 7) | (rd['hour'] > 17)]

	day_ = day.groupby('polarity')['count'].sum()
	night_ = night.groupby('polarity')['count'].sum()

	#chi-2
	contingency = [day_, night_]
	chi2, p, dof, expected = stats.chi2_contingency(contingency)
	print(p)
	
	#Coefficient of Determination
	#r2_score(time, polarity)

	



	#trend of  two kinds of comments in a day
	# pos = rd[rd['polarity'] == 'POG']
	# neg = rd[rd['polarity'] == 'NEG']

	# plt.plot(pos['hour'].values, pos['count'].values)
	# plt.savefig("pos_trend.png")
	# plt.plot(neg['hour'].values, neg['count'].values)
	# plt.savefig("neg_trend.png")
	
	# rd = rd.groupby(['hour' , 'polarity'])['polarity'].count().unstack()
	# # dr2 = dr.groupby(['YearMonth'])['rateRange'].count()
	# rd.plot(kind='bar', stacked = True)
	# plt.savefig('stacked_bar_chat.png')



if __name__ == '__main__':
    main()
