import pandas as pd
import os
import sys

sys.path.insert(0, 'C:/Users/Tom/PycharmProjects/Start/GibHub/My_Libs')
import trading_lib as tl

file = pd.read_excel('earnings/Portfolio123.xlsx')
tickers_list = file['Ticker'].unique()

for t in tickers_list:
    if os.path.isfile('data/yahoo/daily_new/' + str(t) + '.csv'):
        print(t)
    else:
        tl.download_yahoo(t, 'data/yahoo/daily_new/')
