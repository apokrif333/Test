import pandas as pd
import os
import sys

sys.path.insert(0, 'C:/Users/Lex/PycharmProjects/Start/GitHub/My_Libs')
import trading_lib as tl

file = pd.read_excel('earnings/Portfolio123.xlsx')
tickers_list = file['Ticker'].unique()
length = len(tickers_list)

for t in tickers_list:
    index = tickers_list.tolist().index(t)
    print(length - index)
    if os.path.isfile('data/yahoo/daily_new/' + str(t) + '.csv'):
        print(t + ' normal')
    else:
        print(t + ' download')
        tl.download_yahoo(t, 'data/yahoo/daily_new/')
