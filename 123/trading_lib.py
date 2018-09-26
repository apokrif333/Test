# Библиотека функций для трейдов
import pandas as pd
import statistics as stat
import math
import time
import cmath
import os
import matplotlib.pyplot as plt

from alpha_vantage.timeseries import TimeSeries
from yahoofinancials import YahooFinancials
from datetime import datetime
from pprint import pprint as pp


# Constants
ALPHA_KEY = 'FE8STYV4I7XHRIAI'

# Variables
default_data_dir = 'exportTables'  # Директория
start_date = datetime(2017, 1, 1)  # Для yahoo, alpha выкачает всю доступную историю
end_date = datetime.now()

# Globals
alpha_count = 0


# Блок форматированная данных ------------------------------------------------------------------------------------------
# Формат даты в сторку
def dt_to_str(date: datetime) -> str:
    return "%04d-%02d-%02d" % (date.year, date.month, date.day)


# Формат строки в дату
def str_to_dt(string: str) -> datetime:
    return datetime.strptime(string, '%Y-%m-%d')


# Формат листа со строками в лист с датами из файла
def str_list_to_date(file: pd.DataFrame):
    try:
        file["Date"] = pd.to_datetime(file["Date"], dayfirst=False)
    except:
        file["Date"] = pd.to_datetime(file["Date"], format='%d-%m-%Y')


# Округление числа и конвертация его во float
def number_to_float(n) -> float:
    return round(float(n), 2)


# Округление числа и конвертация его в int
def number_to_int(n) -> int:
    return int(round(float(n), 0))


# Не пустой ли объект?
def empty_check(n) -> bool:
    return n is not None and n != 0 and not cmath.isnan(n)


# Блок скачивания цен --------------------------------------------------------------------------------------------------
# Скачиваем нужные тикеры из альфы (не сплитованные цены)
def download_alpha(ticker: str, base_dir: str = default_data_dir) -> pd.DataFrame:
    data = None
    global alpha_count

    try:
        ts = TimeSeries(key=ALPHA_KEY, retries=0)
        data, meta_data = ts.get_daily(ticker, outputsize='full')
    except Exception as err:
        if 'Invalid API call' in str(err):
            print(f'AlphaVantage: ticker data not available for {ticker}')
            return pd.DataFrame({})
        elif 'TimeoutError' in str(err):
            print(f'AlphaVantage: timeout while getting {ticker}')
        else:
            print(f'AlphaVantage: {err}')

    if data is None or len(data.values()) == 0:
        print('AlphaVantage: no data for %s' % ticker)
        return pd.DataFrame({})

    prices = {}
    for key in sorted(data.keys(), key=lambda d: datetime.strptime(d, '%Y-%m-%d')):
        secondary_dic = data[key]
        date = datetime.strptime(key, '%Y-%m-%d')
        dic_with_prices(prices, ticker, date, secondary_dic['1. open'], secondary_dic['2. high'],
                        secondary_dic['3. low'], secondary_dic['4. close'], secondary_dic['5. volume'])

    frame = pd.DataFrame.from_dict(prices, orient='index',
                                   columns=['Open', 'High', 'Low', 'Close', 'Volume', 'Dividend'])
    save_csv(base_dir, ticker, frame, 'alpha')
    time.sleep(15 if alpha_count != 0 else 0)
    alpha_count += 1


# Скачиваем тикеры из яху (цены и дивиденды)
def download_yahoo(ticker: str, base_dir: str = default_data_dir) -> pd.DataFrame:
    try:
        yf = YahooFinancials(ticker)
        data = yf.get_historical_stock_data(dt_to_str(start_date), dt_to_str(end_date), 'daily')
    except Exception as err:
        print(f'Unable to read data for {ticker}: {err}')
        return pd.DataFrame({})

    if data.get(ticker) is None or data[ticker].get('prices') is None or \
            data[ticker].get('timeZone') is None or len(data[ticker]['prices']) == 0:
        print(f'Yahoo: no data for {ticker}')
        return pd.DataFrame({})

    prices = {}
    for rec in sorted(data[ticker]['prices'], key=lambda r: r['date']):
        date = datetime.strptime(rec['formatted_date'], '%Y-%m-%d')
        if rec.get('type') is None:
            dic_with_prices(prices, ticker, date, rec['open'], rec['high'], rec['low'], rec['close'], rec['volume'])
        elif rec['type'] == 'DIVIDEND':
            dic_with_div(prices, ticker, date, rec['amount'])
        elif rec['type'] == 'SPLIT':
            print(f"{ticker} has split {rec['splitRatio']} for {rec['formatted_date']}")

    frame = pd.DataFrame.from_dict(prices, orient='index',
                                   columns=['Open', 'High', 'Low', 'Close', 'Volume', 'Dividend'])
    save_csv(base_dir, ticker, frame, 'yahoo')


# Словарь с ценами
def dic_with_prices(prices: dict, ticker: str, date: datetime, open, high, low, close, volume, dividend=0):
    if date.weekday() > 5:
        print(f'Найден выходной в {ticker} на {date}')
        return

    open = number_to_float(open)
    high = number_to_float(high)
    low = number_to_float(low)
    close = number_to_float(close)
    volume = number_to_int(volume)

    error_price = (not empty_check(open)) or (not empty_check(high)) or (not empty_check(low)) or (
        not empty_check(close))
    error_vol = not empty_check(volume)

    if error_price:
        print(f'В {ticker} на {date} имеются пустые данные')
        return
    if error_vol:
        print(f'В {ticker} на {date} нет объёма')

    prices[date] = [open, high, low, close, volume, dividend]


# Добавляем дивиденды к словарю с ценами
def dic_with_div(prices: dict, ticker: str, date: datetime, amount: float):
    if date.weekday() > 5:
        print(f'Найден выходной в {ticker} на {date}')
        return

    dividend = amount
    error_price = not empty_check(dividend)

    if error_price:
        print(f'В {ticker} на {date} имеются пустые данные в дивидендах')
        return

    prices[date][len(prices[date]) - 1] = dividend


# Блок работы с файлами ------------------------------------------------------------------------------------------------
# Сохраняем csv файл
def save_csv(base_dir: str, file_name: str, data: pd.DataFrame, source: str = 'new_file'):
    path = os.path.join(base_dir)
    if not os.path.exists(path):
        os.makedirs(path)

    if source == 'alpha':
        print(f'{file_name} работает с альфой')
        path = os.path.join(path, file_name + ' NonSplit' + '.csv')
    elif source == 'yahoo':
        print(f'{file_name} работает с яху')
        path = os.path.join(path, file_name + '.csv')
        path = path.replace('^', '')
    elif source == 'new_file':
        print(f'Сохраняем файл с тикером {file_name}')
        path = os.path.join(path, file_name + '.csv')
    else:
        print(f'Неопознанный источник данных для {file_name}')

    if source == 'alpha' or source == 'yahoo':
        data.to_csv(path, index_label='Date')
    else:
        data.to_csv(path, index=False)


# Загружаем csv файл
def load_csv(ticker: str, base_dir: str=default_data_dir) -> pd.DataFrame:
    path = os.path.join(base_dir, str(ticker) + '.csv')
    file = pd.read_csv(path)
    if 'Date' in file.columns:
        str_list_to_date(file)
    return file


# Обрезаем файл согласно определённым датам
def correct_file_by_dates(file: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    return file.loc[(file['Date'] >= start) & (file['Date'] <= end)]


# Блок финансовых метрик -----------------------------------------------------------------------------------------------
# Считаем CAGR
def cagr(date: list, capital: list) -> float:
    years = (date[-1].year + date[-1].month / 12) - (date[0].year + date[0].month / 12)
    return ((capital[-1] / capital[0]) ** (1 / years) - 1) * 100


# Считаем годовое отклонение
def st_dev(capital: list) -> float:
    day_cng = []
    for i in range(len(capital)):
        if i == 0:
            day_cng.append(0)
        else:
            day_cng.append(capital[i] / capital[i - 1] - 1)
    return stat.stdev(day_cng) * math.sqrt(252) if stat.stdev(day_cng) != 0 else 999


# Считаем предельную просадку
def draw_down(capital: list) -> float:
    high = 0
    down = []
    for i in range(len(capital)):
        if capital[i] > high:
            high = capital[i]
        down.append((capital[i] / high - 1) * 100)
    return min(down) if min(down) != 0 else 1


# Блок прочих функций --------------------------------------------------------------------------------------------------
# Ищем самую молодую дату
def newest_date_search(f_date: datetime, *args: datetime) -> datetime:
    newest_date = f_date
    for arg in args:
        if arg > newest_date:
            newest_date = arg
    return newest_date


# Ищем самую старую дату
def oldest_date_search(f_date: datetime, *args: datetime) -> datetime:
    oldest_date = f_date
    for arg in args:
        if arg < oldest_date:
            oldest_date = arg
    return oldest_date


# Рисовалка ------------------------------------------------------------------------------------------------------------
# Выводим график капитала с таблицей
def plot_capital(date: list, capital: list):
    high = 0
    down = []
    for i in range(len(capital)):
        if capital[i] > high:
            high = capital[i]
        down.append((capital[i] / high - 1) * -100)

    names = ['Start Balance', 'End Balance', 'CAGR', 'DrawDown', 'StDev', 'Sharpe', 'MaR', 'SM']
    values = []
    values.append(capital[0])
    values.append(round(capital[-1], 0))
    values.append(round(cagr(date, capital), 2))
    values.append(round(draw_down(capital), 2))
    values.append(round(st_dev(capital) * 100, 2))
    values.append(round(values[2] / values[4], 2))
    values.append(round(abs(values[2] / values[3]), 2))
    values.append(round(values[5] * values[6], 2))

    while len(names) % 4 != 0:
        names.append('')
        values.append('')

    table_metric = pd.DataFrame({'h1': names[:int(len(names) / 4)],
                                 'v1': values[:int(len(values) / 4)],
                                 'h2': names[int(len(names) / 4):int(len(names) / 2)],
                                 'v2': values[int(len(values) / 4):int(len(values) / 2)],
                                 'h3': names[int(len(names) / 2):int(len(names) * 0.75)],
                                 'v3': values[int(len(values) / 2):int(len(values) * 0.75)],
                                 'h4': names[int(len(names) * 0.75):len(names)],
                                 'v4': values[int(len(values) * 0.75):len(values)],
                                 })

    fig = plt.figure(figsize=(12.8, 8.6), dpi=80)

    ax1 = fig.add_subplot(6, 1, (1, 5))
    ax1.plot(date, down, dashes=[6, 4], color="darkgreen", alpha=0.5)
    ax1.set_ylabel('Просадки')

    ax2 = ax1.twinx()
    ax2.plot(date, capital)
    ax2.set_ylabel('Динамика капитала')

    tx1 = fig.add_subplot(6, 1, 6, frameon=False)
    tx1.axis("off")
    tx1.table(cellText=table_metric.values, loc='lower center')

    plt.show()
