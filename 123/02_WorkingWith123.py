import os
import pandas as pd
import math
import trading_lib as tl


default_data_dir = 'exportTables'


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


# Удаляем хвосты с ^
def del_excess():
    file = pd.read_excel('True_File123.xlsx')
    for i in range(len(file)):
        if '^' in file['Ticker'][i]:
            file.loc[i, 'Ticker'] = file['Ticker'][i].split('^', 1)[0]
    file.to_excel('True_File123_New.xlsx')


# Удаляем .
def del_point():
    file = pd.read_excel('True_File123.xlsx')
    for index, row in file.iterrows():
        if '.' in row['Ticker']:
            file.drop(index, inplace=True)
    file = file.reset_index(drop=True)
    file.to_excel('True_File123_New.xlsx')


# Переименовываем файлы
def file_rename():
    path = default_data_dir
    files = os.listdir(path)
    for file in files:
        os.rename(os.path.join(path, file), os.path.join(path, file + '.csv'))


# Проверяем объёмы на сегодня/завтра, чтобы обозначить AMC и BMO


if __name__ == '__main__':

    ''' Создаём единый файл
    files_list = os.listdir()
    del_index = files_list.index('02_WorkingWith123.py')
    del files_list[del_index]
    singe_file(files_list)
    '''

    ''' Скачиваем данные по каждому тикеру
    file = pd.read_excel('True_File123.xlsx')
    tickers_list = file['Ticker'].unique()
    for t in tickers_list:
        if os.path.isfile(os.path.join(default_data_dir, str(t) + '.csv')) is False:
            tl.download_alpha(t)
    '''