import pandas as pd
from datetime import datetime

file_est = pd.read_csv('Final_Estimize_withTosCheck.csv')

print(len(file_est))
for i in range(len(file_est)):
    temp = file_est['date'][i].split('/')
    temp[0] = str(temp[0]) if len(temp[0]) == 2 else '0' + str(temp[0])
    temp[1] = str(temp[1]) if len(temp[1]) == 2 else '0' + str(temp[1])
    file_est.loc[i, 'date'] = str(temp[2]) + '-' + temp[0]  + '-' + temp[1] + str(' 00:00:00')
    print(file_est.loc[i, 'date'])

print(len(file_est['date']))
file_123 = pd.read_excel('Portfolio123.xlsx')
file_123['New_Date'] = file_est['date'].reset_index(drop=True)

file_123.to_excel('New_file.xlsx')