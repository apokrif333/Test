import pandas as pd

from termcolor import colored

# Input
provider = 'alpha' # google yahoo
commisions = 13.3 / 10000 # В числителе: какая комиссия выходит, если разместить и изъять 10 000$ (т.е. на круг)
start = 10000
leverage = 4
position = 10
min_price = 5
max_price = 500
vol_period = 20
min_vol = 100000
max_vol = 1000000

# Switchers
use_filter = 0 # Фильтровка по цене и объёму


# Глобалы
size = 1/position
c_long_yest = 0
dateCorrByReports = 0
enter_list = []
expel_list = []


# Форматируем дату
def date_check(file):
    try:
        file['Date'] = pd.to_datetime(file['Date'])
    except:
        file['date'] = pd.to_datetime(file['date'])


# Добавляем столбик с коэффциентом ранжирования
def dir_coff(file):
    temp = []
    for i in range(len(file['date'])):
        print(file['date'][i])

        eps_a = file['epsAct'][i]
        eps_e = file['epsEst'][i]
        rev_a = file['revAct'][i]
        rev_e = file['revEst'][i]

        if(eps_a and eps_e and rev_a and rev_e) != 0:
            if (eps_a > eps_e) and (rev_a > rev_e):
                temp.append(((eps_a - eps_e) / abs(eps_e) + 1) * ((rev_a - rev_e) / abs(rev_e) + 1))
            elif (eps_a < eps_e) and (rev_a < rev_e):
                temp.append(abs(((eps_a - eps_e) / abs(eps_e) - 1) * ((rev_a - rev_e) / abs(rev_e) - 1)) * -1)
            else:
                temp.append(0)
        else:
            temp.append(0)
    file['dirCoff'] = temp
    file.to_excel('earnings/portfolio_123_Corr.xlsx')


# Меняем даты у отчётов так, чтобы дата соотвествовала дню входа
def report_time(file):
    temp = []
    for i in range(len(file['date'])):
        print(file['date'][i])

        if file['reports'][i] == 'AMC':
            cur_ticker = pd.read_csv('C:/Users/Tom/PycharmProjects/Start/05_StockResearch/data/alpha/daily/' +
                                     str(file['ticker'][i]) + '.csv')
            cur_ticker['Date'] = pd.to_datetime(cur_ticker['Date'])
            date_index = cur_ticker.loc[cur_ticker['Date'] == file['date'][i]].index.values + 1
            int_index = int(date_index[0])
            temp.append(cur_ticker['Date'][int_index])

        else:
            temp.append(file['date'][i])

    file['dateCorrByReports'] = temp
    file = file.sort_values(by=['dateCorrByReports'])
    file.to_excel('earnings/portfolio_123_Corr.xlsx')

# Возле каждого тикера, у котрого есть ранк, создаём %cng для шорта, лонга однодневного и двухдневного
# def all_enters(file):


# Возвращаем результат сделки по каждому тикеру
def enter_to_poz(ticker, date, dir):
    file_t = pd.read_csv('data/' + str(provider) + '/daily/'+str(ticker)+'.csv')
    date_check(file_t)

    try:
        open = float(file_t['Open'].loc[(file_t['Date'] == date)])
    except:
        empty_date = ('Невозможно найти дату ' + str(date) + ". Тикер " + str(ticker))
        expel_list.append(empty_date)
        return 0
        # file_t = pd.read_csv('data/' + 'google' + '/daily/' + str(ticker) + '.csv')
        # date_check(file_t)
        # open = float(file_t['Open'].loc[(file_t['Date'] == date)])

    if dir == 1:
        close = file_t['Close'][file_t['Date'].tolist().index(date) + 1]
    elif dir == -1:
        close = file_t['Close'][file_t['Date'].tolist().index(date)]
    else:
        raise Exception(colored('Передано неверное направление сделки', 'red'))

    total = ((close / open - 1) - commisions) * leverage
    enters = ('Enter: ' + str(date) + '; Ticker: ' + str(ticker) + '; Direction: ' + str(dir) + '; Change: ' + str(total))
    enter_list.append(enters)

    return total


# Проверка цены и объёма
def price_vol(file, main_date):
    if len(file) == 0:
        return file

    for t in file['ticker']:
        file_t = pd.read_csv('data/' + str(provider) + '/daily/' + str(t) + '.csv')
        date_check(file_t)

        try:
            cur_index = file_t['Date'].tolist().index(main_date)
            open = float(file_t['Open'][cur_index])
            volume = file_t['Volume'].rolling(vol_period).mean()
            cur_volume = float(volume[cur_index])
        except:
            empty_date = ('Невозможно найти дату ' + str(main_date) + ". Тикер " + str(t))
            expel_list.append(empty_date)
            for index, element in file.iterrows():
                if element['ticker'] == t:
                    file = file.drop(index)
            continue

        if (open < min_price or open > max_price) or (cur_volume < min_vol or cur_volume > max_vol):
            for index, element in file.iterrows():
                if element['ticker'] == t:
                    expel = (str(main_date) + ' Исключён тикер: ' + str(t) + '. Или цена, или объём')
                    expel_list.append(expel)
                    file = file.drop(index)

    return file


def main(file, dates):
    day_change = []
    long_list = []
    short_list = []
    capital = []

    # Перебираем все уникальные даты, кроме последней
    for d in range(len(dates)-1):
        global c_long_yest
        global dateCorrByReports

        # Формируем файлы на дату входа и сортируем их по большему разрыву Act-Est
        long_tickers = file.loc[(file['dateCorrByReports'] == dates[d]) & (file['dirCoff'] > 0)].\
            sort_values(by=['dirCoff'], ascending=False)
        short_tickers = file.loc[(file['dateCorrByReports'] == dates[d]) & (file['dirCoff'] < 0)].\
            sort_values(by=['dirCoff'])

        # Определяем дату, на которую будут совершаться сделки
        dateCorrByReports = dates[d]

        print(dateCorrByReports)
        print(dates[d])
        # Фильтруем акции
        if use_filter == 1:
            long_tickers = price_vol(long_tickers, dateCorrByReports)
            short_tickers = price_vol(short_tickers, dateCorrByReports)

        # Расчёт соотношений лонгов к шортам сегодня
        c_long = len(long_tickers)
        c_short = 0 # len(short_tickers)
        if (c_long + c_short) > 0:
            l_perc = c_long / (c_long + c_short)
            s_perc = 1 - l_perc
        else:
            l_perc = 0
            s_perc = 0

        # Расчёт %cng для каждой сделки
        longs = 0
        shorts = 0
        if c_long + c_short + c_long_yest <= position and c_long + c_short > 0:
            for t in long_tickers['ticker']:
                if c_long == 0:
                    break
                longs = enter_to_poz(t, dateCorrByReports, 1) * size + longs
            for t in short_tickers['ticker']:
                if c_short == 0:
                    break
                shorts = enter_to_poz(t, dateCorrByReports, -1) * size + shorts

        elif (c_long or c_short) != 0:
            if c_long > 0:
                long_for_enter = int(round((position - c_long_yest) * l_perc))
                long_tickers = long_tickers.head(long_for_enter)
                for t in long_tickers['ticker']:
                    longs = enter_to_poz(t, dateCorrByReports, 1) * size + longs

            if c_short > 0:
                short_for_enter = int(round((position - c_long_yest) * s_perc))
                short_tickers = short_tickers.head(short_for_enter)
                for t in short_tickers['ticker']:
                    shorts = enter_to_poz(t, dateCorrByReports, -1) * size + shorts

        c_long_yest = len(long_tickers['ticker'])
        # c_long_yest = longs

        long_list.append(len(long_tickers))
        short_list.append(len(short_tickers))
        day_change.append(longs+shorts)
        capital.append(day_change[-1] * capital[-1]  + capital[-1] if len(capital) > 0 else
                       day_change[-1] * start + start)

    cap_table = pd.DataFrame({'Date': dates[:-1],
                              'Long': long_list,
                              'Short': short_list,
                              'Change': day_change,
                              'Capital': capital,})
    enter_table = pd.DataFrame({'Enter': enter_list})
    expel_table = pd.DataFrame({'Errors': expel_list})


    cap_table.to_excel('my folder/Tic ' + str(position) + '; Lev ' + str(leverage) + '; Comm '+ str(commisions) +
                       '; withFilters' + '.xlsx')
    enter_table.to_excel('my folder/Enter_Table.xlsx')
    expel_table.to_excel('my folder/Error_Table.xlsx')


if __name__ == '__main__':
    earn_file = pd.read_excel('earnings/portfolio_123_Corr.xlsx')
    un_dates = earn_file['date'].unique()

    main(earn_file, un_dates)