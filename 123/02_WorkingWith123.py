import os
import pandas as pd
import math
from alpha_vantage.timeseries import TimeSeries

alpha_vantage_key = 'FE8STYV4I7XHRIAI'


# Создаём единый файл
def singe_file(files_list):
    main_file = pd.DataFrame({})
    for f in files_list:
        print(f)
        file = pd.read_excel(str(f), header=3)
        main_file = pd.concat([main_file, file], sort=False, ignore_index=True)

    # main_file = main_file.drop(columns=['Unnamed: 13', 'Unnamed: 12'])
    main_file.to_excel('Total_File123.xlsx')


# Удаляем все строчки, где нехватка данных или дата 1970 год
def delete_bad_rows():
    file = pd.read_excel('Total_File123.xlsx')

    for i in range(len(file)):
        if math.isnan(file['@est_eps'][i]) or math.isnan(file['@act_eps'][i]) or \
                math.isnan(file['@est_sales'][i]) or math.isnan(file['@act_sales'][i]) or \
                file['@date_'][i] == 19700101:
            file = file.drop(i)

    file[''] = [x for x in range(len(file))]
    # file = file.drop(columns='Unnamed: 0').set_index('')
    if 'Price' in file.columns:
        file = file.drop(columns='Price')
    if 'LatestNewsDate' in file.columns:
        file = file.drop(columns='LatestNewsDate')
    if 'Last' in file.columns:
        file = file.drop(columns='Last')

    file = file.reset_index(drop=True)
    file.to_excel('Total_File123_New.xlsx')


# Корректируем числовую дату, в нормальный формат даты
def date_correct():
        temp = []
        file = pd.read_excel('Total_File123.xlsx')
        file['@date_'] = file['@date_'].apply(str)

        for i in range(len(file['Ticker'])):
            temp.append(
                pd.datetime(int(file['@date_'][i][:4]), int(file['@date_'][i][4:6]), int(file['@date_'][i][6:8])))
        file['@date_'] = temp

        file.to_excel('Total_File123_New.xlsx')


# Ищем в едином файле дубликаты
def del_duplicate():
    indexes_for_drop = []
    file = pd.read_excel('Total_File123.xlsx')
    # print(file.index[file['Ticker'] == file['Ticker'][5000]].tolist())

    for i in range(len(file)):
        rows_for_del = file.loc[(file['Ticker'] == file['Ticker'][i]) & (file['@date_'] == file['@date_'][i])]
        index_list = list(rows_for_del.index.values)
        print(index_list)

        temp = 1
        while temp < len(index_list):
            indexes_for_drop.append(index_list[temp])
            temp += 1

    indexes_for_drop = list(set(indexes_for_drop))
    for i in range(len(indexes_for_drop)):
        print(str(len(indexes_for_drop)) + ' | ' + str(i))
        file = file.drop(indexes_for_drop[i])

    file = file.reset_index(drop=True)
    file.to_excel('True_File123.xlsx')


# Работаем с alphavantage
def alpha_data(ticker):
    data = None

    file_list = os.listdir('data/alpha/daily')
    for i in range(len(file_list)):
        file_list[i] = os.path.splitext(file_list[i])[0]
    if ticker in file_list:
        return

    try:
        ts = TimeSeries(key=alpha_vantage_key, retries=0)
        data, meta_data = ts.get_daily(ticker, outputsize='full')
    except Exception as err:
        if 'Invalid API call' in str(err):
            print('AlphaVantage: ticker data not available for %s' % ticker)
            return

    print(data)

# Проверяем объёмы на сегодня/завтра, чтобы обозначить AMC и BMO
def vol_check(file):
    ticker_list = list(set(file['Ticker']))
    ticker_list = [x for x in ticker_list if '.' not in x]
    for i in range(len(ticker_list)):
        if '^' in ticker_list[i]:
            ticker_list[i] = ticker_list[i].split('^')[0]
    ticker_list = list(filter(lambda x: x.isalpha(), ticker_list))

    for ticker in ticker_list:
        alpha_data(ticker)


if __name__ == '__main__':

    ''' Создаём единый файл
    files_list = os.listdir()
    del_index = files_list.index('02_WorkingWith123.py')
    del files_list[del_index]
    singe_file(files_list)
    '''

    del_duplicate()
    # file = pd.read_excel('earnings/123_Earnings_Data/NewFiles/True_File123.xlsx')
    # vol_check(file)
