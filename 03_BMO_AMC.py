import pandas as pd
from termcolor import colored


# Проверка по объёму и возратат True or False
def volume(ticker, date):
    print(date)
    try:
        file = pd.read_csv('data/alpha/daily/' + str(ticker) + '.csv')
        file['Date'] = pd.to_datetime(file['Date'])
    except:
        print(colored('Невозможно считать файл с тикером: ' + str(ticker) + ' и датой: ' + str(date), 'magenta'))
        return 0

    today_vol = file['Volume'].loc[file['Date'] == date]
    tomorr_vol = file['Volume'][today_vol.index.values + 1]

    try:
        today_vol = int(today_vol.reset_index(drop=True))
        tomorr_vol = int(tomorr_vol.reset_index(drop=True))
    except:
        print(colored('Искомой даты нет для тикера: ' + str(ticker) + ' и датой: ' + str(date), 'red'))
        return 0

    if today_vol > tomorr_vol:
        return 1
    elif today_vol < tomorr_vol:
        return -1


# Построчно, для каждого тикера, на каждую дату ищем объёмы и создаём колонку с BMO и AMC
def check_time(file):
    temp = []
    for i in range(len(file)):
        cur_ticker = file['ticker'][i]
        cur_date = file['date'][i]

        news = volume(cur_ticker, cur_date)
        if news == 1:
            temp.append('BMO')
        elif news == -1:
            temp.append('AMC')
        else:
            temp.append('Empty')

    file['checkReports'] = temp
    file.to_excel('earnings/EarningsEstimizeVsTOS_reports.xlsx')


if __name__ == '__main__':
    file = pd.read_excel('earnings/EarningsEstimizeVsTOS.xlsx')
    check_time(file)