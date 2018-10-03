import atexit
import cmath
import datetime
import hashlib
import multiprocessing
import os
import pickle
import sys
import time
import typing
import math
import re

import alpha_vantage.timeseries
import colorama
import dotmap
import googlefinance.client
import holidays
import pandas
import termcolor
from yahoofinancials import YahooFinancials
import matplotlib.pyplot as plt

# Initialize colored output
colorama.init(strip=False)


# Event class
class Event:
    # Types
    EPS_ONLY = 'eps_only'
    EPS_AND_REV = 'eps_rev'

    # Field list
    __slots__ = [
        'date',
        'ticker',
        'reports',
        'eps_con',
        'eps_act',
        'rev_con',
        'rev_act',
        'type',
        'entry_date',
        'next_date',
        'attrs'
    ]

    # Constructor
    def __init__(self,
                 ticker: str,
                 date: datetime,
                 reports: str,
                 report_type: str,
                 eps_con: float = None,
                 eps_act: float = None,
                 rev_con: float = None,
                 rev_act: float = None):

        self.ticker = ticker
        self.date = date
        self.reports = reports
        self.type = report_type
        self.eps_con = eps_con
        self.eps_act = eps_act
        self.rev_con = rev_con
        self.rev_act = rev_act
        self.entry_date = None
        self.next_date = None
        self.attrs = dotmap.DotMap()

    # EPS change
    @property
    def eps_change(self) -> typing.Optional[float]:
        if self.eps_con is not None and self.eps_con != 0 and self.eps_act is not None and self.eps_act != 0:
            return (self.eps_act - self.eps_con) / abs(self.eps_con)
        else:
            return None

    # Revenue change
    @property
    def rev_change(self) -> typing.Optional[float]:
        if self.rev_con is not None and self.rev_con != 0 and self.rev_act is not None and self.rev_act != 0:
            return (self.rev_act - self.rev_con) / abs(self.rev_con)
        else:
            return None

    # Check if event valid
    @property
    def is_valid(self) -> bool:
        # Check data
        if self.type == Event.EPS_ONLY:
            return self.eps_con is not None and self.eps_act is not None
        elif self.type == Event.EPS_AND_REV:
            return self.eps_con is not None and \
                   self.eps_act is not None and \
                   self.rev_con is not None and \
                   self.rev_act is not None


# Trade class
class Trade:
    # Directions
    LONG = 'long'
    SHORT = 'short'

    # Field list
    __slots__ = [
        'id',
        'ticker',
        'entry_date',
        'exit_date',
        'direction',
        'price',
        'stop',
        'volume',
        'margin_used',
        'day_trade',
        'pos_risk'
    ]

    # Constructor
    def __init__(self,
                 trade_id: int,
                 ticker: str,
                 entry_date: datetime,
                 exit_date: datetime,
                 direction: str,
                 price: float,
                 stop: float,
                 volume: int,
                 margin_used: float = 0,
                 pos_risk: float = 0):
        self.id = trade_id
        self.ticker = ticker
        self.entry_date = entry_date
        self.exit_date = exit_date
        self.direction = direction
        self.price = price
        self.stop = stop
        self.volume = volume
        self.day_trade = self.entry_date == self.exit_date
        self.margin_used = margin_used
        self.pos_risk = pos_risk

    @property
    def type_str(self) -> str:
        return '' if self.day_trade else 'overnight '


# Base strategy class
class Strategy:
    # Data sources
    DATA_YAHOO = 'yahoo'
    DATA_GOOGLE = 'google'
    DATA_ALPHA = 'alpha'

    # Event sources
    EVENTS_ESTIMIZE = 'estimize'
    EVENTS_TOS = 'tos'
    EVENTS_ZACKS = 'zacks'
    EVENTS_ESTIMIZE_TOS = 'estimize_tos'
    EVENTS_ESTIMIZE_FINAL = 'estimize_final'
    EVENTS_IB = 'ib'
    EVENTS_PORTFOLIO_123 = 'portfolio_123'

    # Events files
    EVENT_FILE = {
        EVENTS_ESTIMIZE: 'earnings/Earnings.xlsx',
        EVENTS_TOS: 'earnings/EarningsTOS.xlsx',
        EVENTS_ZACKS: 'earnings/EarningsZacks.xlsx',
        EVENTS_ESTIMIZE_TOS: 'earnings/EarningsEstimizeVsTOS.xlsx',
        EVENTS_ESTIMIZE_FINAL: 'earnings/Final_Estimize_withTosCheck.csv',
        EVENTS_IB: 'earnings/IBReports.xlsx',
        EVENTS_PORTFOLIO_123: 'earnings/Portfolio123.xlsx'
    }

    # Timeframes
    TF_DAILY = 'daily'

    # Exclude this tickers
    EXCLUDE_TICKERS = ['PRN']

    # Broker types
    BROKER_IB_TIERED = 'IB Tiered'
    BROKER_IB_CFD = 'IB CFD'
    BROKER_IB_CFD_STRICT = 'IB CFD (strict)'
    BROKER_FONDEXX = 'Fondexx'

    # US holidays
    US_HOLIDAYS = holidays.UnitedStates(observed=False)

    # Key
    ALPHA_ACCESS_KEY = 'FE8STYV4I7XHRIAI'

    # Default ECN + other comissions per share
    DEFAULT_MARKET_FEES = 0.004

    # Log categories
    LOG_NO_BAR_DATA = 'no_bar_data'
    LOG_ERROR_BAR_DATA = 'error_bar_data'
    LOG_HYPERCACHE = 'hypercache'
    LOG_DOWNLOAD = 'history_download'
    LOG_DOWNLOAD_ERROR = 'history_download_error'
    LOG_BROKER = 'broker'
    LOG_DAY_BORDERS = 'day_borders'
    LOG_TRADE = 'trade'
    LOG_TRADE_ERROR = 'trade_error'
    LOG_EVENT = 'event'
    LOG_EVENT_ERROR = 'event_error'
    LOG_EVENT_HOLIDAY = 'event_holiday'

    # Global data cache
    DATA_CACHE = {}

    # Others
    OUTPUT_PADDING = 40
    PLOT_SIZE = {'figsize': (12.8, 9.6), 'dpi': 80}
    LOG_STARTED = {}

    # Field list
    __slots__ = [
        'name',
        'log_file_name',
        'log_file_obj',
        'log_time',
        'log_disable_categories',
        'broker',
        'ib_cfd_list',
        'ib_cfd_list_path',
        'data_base_dir',
        'event_source',
        'events',
        'data_source',
        'data_disable_download',
        'empty_frame',
        'balance_start',
        'date_start',
        'date_end',
        'date_current',
        'balance',
        'gross_balance',
        'balance_series',
        'drawdown_series',
        'day_margin',
        'overnight_margin',
        'day_trades',
        'overnight_trades',
        'current_overnight_trades',
        'day_start_balance',
        'slippage',
        'commission_total',
        'slippage_total',
        'reached_stop_count',
        'total_volume',
        'total_winnings',
        'total_losings',
        'total_trades',
        'long_trades',
        'short_trades',
        'long_wins',
        'short_wins',
        'winning_streak',
        'losing_streak',
        'max_winning_streak',
        'max_losing_streak',
        'balance_high',
        'holiday_event_count',
        'missed_bar_count',
        'error_bar_count',
        'valid_events_count',
        'out_lines',
        'user_lines',
        'no_day_margin_count',
        'no_overnight_margin_count',
    ]

    # Constructor
    def __init__(self,
                 name: str = 'Strategy 1',
                 data_source: str = DATA_ALPHA,
                 event_source: str = None,
                 data_base_dir: str = 'data',
                 broker: str = BROKER_FONDEXX,
                 log_file: str = sys.argv[0].lower().replace('.py', '.log'),
                 disable_download: bool = False,
                 ib_cfd_list_path: str = 'misc/IB_CFD_Shares.xlsx',
                 log_time: bool = True,
                 balance_start: int = 10000,
                 date_start: datetime = datetime.datetime(2012, 1, 1),
                 date_end: datetime = datetime.datetime.now(),
                 day_margin: int = 1,
                 overnight_margin: int = 1,
                 slippage: float = 0.0,
                 exclude_logs: list = ()):

        # Internals
        self.name = name
        self.broker = broker
        self.ib_cfd_list = None
        self.ib_cfd_list_path = ib_cfd_list_path
        self.event_source = event_source
        self.events = {}
        self.data_source = data_source
        self.data_base_dir = data_base_dir
        self.data_disable_download = disable_download
        self.empty_frame = self._make_data_frame({})

        # Properties
        self.balance_start = balance_start
        self.date_start = date_start
        self.date_end = date_end
        self.date_current = None
        self.balance = balance_start
        self.gross_balance = balance_start
        self.day_start_balance = balance_start
        self.balance_series = pandas.Series()
        self.drawdown_series = pandas.Series()
        self.day_margin = day_margin
        self.overnight_margin = overnight_margin
        self.day_trades = []
        self.overnight_trades = []
        self.current_overnight_trades = []
        self.slippage = slippage
        self.slippage_total = 0
        self.commission_total = 0
        self.reached_stop_count = 0
        self.total_volume = 0
        self.total_winnings = 0
        self.total_losings = 0
        self.total_trades = 0
        self.long_trades = 0
        self.short_trades = 0
        self.long_wins = 0
        self.short_wins = 0
        self.winning_streak = 0
        self.losing_streak = 0
        self.max_winning_streak = 0
        self.max_losing_streak = 0
        self.balance_high = 0
        self.holiday_event_count = 0
        self.missed_bar_count = 0
        self.error_bar_count = 0
        self.valid_events_count = 0
        self.out_lines = []
        self.user_lines = []
        self.no_day_margin_count = 0
        self.no_overnight_margin_count = 0

        # Initiaize logging
        self.log_file_name = log_file
        self.log_file_obj = None

        self.log_disable_categories = exclude_logs
        self.log_time = log_time

        # Open file name if needed
        if self.log_file_name is not None:
            # Open if needed
            self.log_file_obj = open(self.log_file_name, 'a' if Strategy.LOG_STARTED.get(self.log_file_name) else 'w')
            # Set started state
            Strategy.LOG_STARTED[self.log_file_name] = True
            # Register exit handler
            atexit.register(self._close_logging)

    # Close logging
    def _close_logging(self):
        if self.log_file_obj:
            self.log_file_obj.flush()
            self.log_file_obj.close()
            self.log_file_obj = None

    # Format current datetime
    def _log_time(self) -> str:
        return '' if not self.log_time else (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' ')

    # Log to file
    def _log_file(self, msg: str):
        # Write message
        if self.log_file_obj:
            self.log_file_obj.write('%s\n' % msg)

    # Print message
    def _log_print(self, level: str, msg: str, color: str, attrs: [] = (), category: str = None):
        if category not in self.log_disable_categories:
            text = '%s%s %s' % (self._log_time(), level, msg)
            termcolor.cprint(text, color, attrs=attrs)
            self._log_file(msg=text)

    # Print error
    def log_error(self, msg: str, category: str = None):
        self._log_print(
            level='ERROR',
            msg=msg,
            color='red',
            category=category)

    # Fatal error
    def log_fatal(self, msg: str, category: str = None):
        self._log_print(
            level='FATAL',
            msg=msg,
            color='red',
            attrs=['bold'],
            category=category)
        exit()

    # Warning
    def log_warn(self, msg: str, category: str = None):
        self._log_print(
            level='WARN',
            msg=msg,
            color='yellow',
            category=category)

    # Info
    def log_info(self, msg: str, category: str = None):
        self._log_print(
            level='INFO',
            msg=msg,
            color='green',
            category=category)

    # Buy
    def log_buy(self, msg: str, category: str = None):
        self._log_print(
            level='BUY',
            msg=msg,
            color='blue',
            category=category)

    # Sell
    def log_sell(self, msg: str, category: str = None):
        self._log_print(
            level='SELL',
            msg=msg,
            color='magenta',
            category=category)

    # Short
    def log_short(self, msg: str, category: str = None):
        self._log_print(
            level='SHORT',
            msg=msg,
            color='magenta',
            category=category)

    # Exit short
    def log_cover(self, msg: str, category: str = None):
        self._log_print(
            level='COVER',
            msg=msg,
            color='blue',
            category=category)

    # Preprocess history
    def history_preprocess(self, data: pandas.DataFrame):
        pass

    # Justify text left to padding
    @staticmethod
    def _pad(text: str) -> str:
        return text.ljust(Strategy.OUTPUT_PADDING)

    # Add main table info
    def tprint(self, header: str = None, value: str = None):
        self.out_lines.append({'header': header, 'value': value})

    # Add user additional info lines
    def add_line(self, header: str = None, value: str = None):
        self.user_lines.append({'header': header, 'value': value})

    # Safe division
    @staticmethod
    def _safe_div(a, b):
        return 0 if b == 0 else a / b

    # Convert datetime to ISO date string
    @staticmethod
    def d2str(date: datetime) -> str:
        return date.date().isoformat()

    # Check if day valid
    @staticmethod
    def is_holiday(date: datetime) -> str:
        if date.weekday() == 5:
            return 'Weekend (Saturday)'
        elif date.weekday() == 6:
            return 'Weekend (Sunday)'
        else:
            return Strategy.US_HOLIDAYS.get(date)

    # Search next trade day
    @staticmethod
    def get_next_trade_day(date_from: datetime) -> datetime:
        while True:
            date_from += datetime.timedelta(days=1)
            if Strategy.is_holiday(date_from) is None:
                return date_from

    # Get broker comission for trade
    def get_comission(self, volume: int, price: float) -> float:
        if self.broker == self.BROKER_IB_TIERED:
            commission = max(0.35, volume * 0.0035)
            commission = min(commission, volume * price)
            return commission + volume * self.DEFAULT_MARKET_FEES
        elif self.broker == self.BROKER_IB_CFD or self.broker == self.BROKER_IB_CFD_STRICT:
            return max(1.0, volume * 0.005)
        elif self.broker == self.BROKER_FONDEXX:
            return volume * (0.003 + self.DEFAULT_MARKET_FEES)
        else:
            self.log_fatal('Unsuppored broker type %s' % self.broker, category=self.LOG_BROKER)

    # Adjust volume
    def get_adjusted_volume(self, volume: int) -> int:
        if self.broker == self.BROKER_IB_CFD or \
                self.broker == self.BROKER_IB_TIERED or \
                self.broker == self.BROKER_IB_CFD_STRICT:
            return volume
        elif self.broker == self.BROKER_FONDEXX:
            return (volume // 100) * 100
        else:
            self.log_fatal('Unsuppored broker type %s' % self.broker, category=self.LOG_BROKER)

    # Check if ticker available (CFD list check)
    def is_stock_available(self, ticker: str) -> bool:
        if self.broker != self.BROKER_IB_CFD_STRICT:
            return True

        if self.ib_cfd_list is None:
            self.ib_cfd_list = {}
            for index, row in pandas.read_excel(self.ib_cfd_list_path).iterrows():
                if row[0] == 'Share':
                    self.ib_cfd_list[str(row[1])] = 1

        return self.ib_cfd_list.get(ticker) is not None

    # Check if price correct
    @staticmethod
    def _check_price(n) -> bool:
        return n is not None and n != 0 and not cmath.isnan(n)

    # Check if volume correct
    @staticmethod
    def _check_volume(n) -> bool:
        return n is not None and not cmath.isnan(n)

    # Make stock price
    @staticmethod
    def _make_price(n) -> float:
        return round(float(n), 2) if n is not None else None

    # Make stock volume
    @staticmethod
    def _make_volume(n) -> int:
        return int(round(float(n), 0)) if n is not None else None

    # Check price and add to prices list
    def _check_add_price(self, prices: dict, ticker: str, date: datetime, v_open, v_high, v_low, v_close, v_volume):
        # Check week day
        if date.weekday() > 5:
            self.log_error('Skipped weekend date %s for %s' % (self.d2str(date), ticker))
            return

        # Get price parts
        r_open = self._make_price(v_open)
        r_high = self._make_price(v_high)
        r_low = self._make_price(v_low)
        r_close = self._make_price(v_close)
        r_volume = self._make_volume(v_volume)

        # Check error
        error = (not self._check_price(r_open)) or \
                (not self._check_price(r_high)) or \
                (not self._check_price(r_low)) or \
                (not self._check_price(r_close)) or \
                (not self._check_volume(r_volume))

        # Add price
        prices[date] = [r_open, r_high, r_low, r_close, r_volume, error]

    # Create directory if needed and get data path
    def _get_data_path(self, period: str = None) -> str:
        if period:
            path = os.path.join(self.data_base_dir, self.data_source, period)
        else:
            path = os.path.join(self.data_base_dir, self.data_source)

        if not os.path.exists(path):
            os.makedirs(path)

        return path

    # Get data file name (create directory if needed)
    def _get_data_file(self, period: str, ticker: str) -> str:
        return os.path.join(self._get_data_path(period), ticker + '.csv')

    # Create compatible dataframe
    @staticmethod
    def _make_data_frame(source: dict) -> pandas.DataFrame:
        return pandas.DataFrame.from_dict(
            source,
            orient='index',
            columns=['Open', 'High', 'Low', 'Close', 'Volume', 'Error'])

    # Get ticker data and cache it
    def get_daily_data(self, ticker: str) -> pandas.DataFrame:
        # Check if loaded
        data = None
        ticker = str(ticker).upper()
        ticker_dict = Strategy.DATA_CACHE.get(self.data_source)
        if ticker_dict is not None:
            data = ticker_dict[self.TF_DAILY].get(ticker)
            if data is not None:
                return data

        # Load
        if self.data_source == self.DATA_YAHOO:
            data = self.download_days_yahoo(ticker)
        elif self.data_source == self.DATA_GOOGLE:
            data = self.download_days_google(ticker)
        elif self.data_source == self.DATA_ALPHA:
            data = self.download_days_alpha(ticker)
        else:
            self.log_fatal('Unknown data provider: %s' % self.data_source, category=self.LOG_DOWNLOAD_ERROR)

        # Post-process
        self.history_preprocess(data)

        # Store in cache
        Strategy.DATA_CACHE.setdefault(self.data_source, {}).setdefault(self.TF_DAILY, {})[ticker] = data
        return data

    # Get day bar for ticker
    def get_day_data(self, ticker: str, date: datetime) -> typing.Optional[dotmap.DotMap]:
        bars = self.get_daily_data(ticker)
        if date in bars.index:
            return dotmap.DotMap(bars.loc[date].to_dict())
        else:
            self.log_error('No %s day data for %s' % (self.d2str(date), ticker), category=self.LOG_NO_BAR_DATA)
            return None

    # Load data from csv
    def load_csv(self, period: str, ticker: str) -> pandas.DataFrame:
        file = self._get_data_file(period, ticker)
        return pandas.read_csv(file, index_col=0, dtype={'Error': 'bool'}, parse_dates=['Date'])

    # Save data frame to csv
    def save_csv(self, period: str, ticker: str, data: pandas.DataFrame):
        file = self._get_data_file(period, ticker)
        frame = data.copy(deep=True)
        frame['Error'] = frame['Error'].map({True: 1, False: 0})
        frame.to_csv(file, index_label='Date')

    # Calculate directory hash
    def _hash_data_dir(self) -> str:
        m = hashlib.md5()
        path = self._get_data_path(self.TF_DAILY)

        for file in os.listdir(path):
            if file.endswith('.csv'):
                info = os.stat(os.path.join(path, file))
                info_str = '%d, %f' % (info.st_size, info.st_mtime)
                m.update(info_str.encode())

        return m.hexdigest()

    # Save hyper cache
    def save_hyper_cache(self):
        dir_hash = self._hash_data_dir()
        path = self._get_data_path()
        file_name = os.path.join(path, 'daily_%s.dat' % dir_hash)

        # Scan for generated caches
        save = True
        for file in os.listdir(path):
            if file == file_name:
                save = False
                continue

            # Remove other caches
            if file.startswith('daily_'):
                os.remove(os.path.join(path, file))

        # Save if needed
        if save:
            self.log_info('Saving updated %s hyper cache' % self.data_source, category=self.LOG_HYPERCACHE)
            with open(file_name, 'wb') as handle:
                pickle.dump(Strategy.DATA_CACHE[self.data_source], handle, protocol=pickle.HIGHEST_PROTOCOL)

    # Preload hyper cache
    def load_hyper_cache(self):
        # Check if data already loaded
        if Strategy.DATA_CACHE.get(self.data_source) is not None:
            return

        # Check for cache file
        dir_hash = self._hash_data_dir()
        path = self._get_data_path()
        file_name = os.path.join(path, 'daily_%s.dat' % dir_hash)

        # Load if cache exists
        if os.path.exists(file_name):
            self.log_info('Preloading %s hyper cache' % self.data_source, category=self.LOG_HYPERCACHE)
            with open(file_name, 'rb') as handle:
                Strategy.DATA_CACHE[self.data_source] = pickle.load(handle)
                return

        # Read all data
        self.log_info(
            'Hyper cache for %s not exists or outdated - generating fresh one' % self.data_source,
            category=self.LOG_HYPERCACHE)

        for file in os.listdir(self._get_data_path(self.TF_DAILY)):
            file = file.replace('.csv', '')
            self.get_daily_data(file)

        # Save hyper cache
        self.save_hyper_cache()

    # Download tickers from alpha vantage
    def download_days_alpha(self, ticker: str) -> pandas.DataFrame:
        if ticker in self.EXCLUDE_TICKERS:
            return self.empty_frame

        # Check if data exists
        ticker = str(ticker).upper()
        file = self._get_data_file(self.TF_DAILY, ticker)
        if os.path.isfile(file):
            return self.load_csv(self.TF_DAILY, ticker)

        if self.data_disable_download:
            return self.empty_frame

        try_count = 0
        stop = False
        data = None
        while not stop:
            try:
                # Get parsed data
                try_count += 1
                time.sleep(15)
                self.log_info(
                    'Downloading alpha vantage daily data for %s%s' %
                    (ticker, (' (try %d)' % try_count) if try_count > 1 else ''),
                    category=self.LOG_DOWNLOAD)

                ts = alpha_vantage.timeseries.TimeSeries(key=Strategy.ALPHA_ACCESS_KEY, retries=0)
                data, meta_data = ts.get_daily(ticker, outputsize='full')
                stop = True
                time.sleep(15)
            except Exception as err:
                if 'Invalid API call' in str(err):
                    self.log_warn(
                        'AlphaVantage: ticker data not available for %s' % ticker,
                        category=self.LOG_DOWNLOAD_ERROR)
                    return self.empty_frame
                elif 'TimeoutError' in str(err):
                    self.log_warn(
                        'AlphaVantage: timeout while getting %s' % ticker,
                        category=self.LOG_DOWNLOAD_ERROR)
                else:
                    self.log_error(
                        'AlphaVantage: %s' % err,
                        category=self.LOG_DOWNLOAD_ERROR)

                if try_count > 5:
                    self.log_warn(
                        'AlphaVantage: maximum try count reached for %s' % ticker,
                        category=self.LOG_DOWNLOAD_ERROR)
                    return self.empty_frame

        # Return empty dataset
        if data is None or len(data.values()) == 0:
            self.log_warn('AlphaVantage: no data for %s' % ticker, category=self.LOG_DOWNLOAD_ERROR)
            return self.empty_frame

        prices = {}
        for key in sorted(data.keys(), key=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d')):
            rec = data[key]
            date = datetime.datetime.strptime(key, '%Y-%m-%d')
            self._check_add_price(
                prices, ticker, date,
                rec['1. open'], rec['2. high'], rec['3. low'], rec['4. close'], rec['5. volume'])

        frame = self._make_data_frame(prices)
        self.save_csv(self.TF_DAILY, ticker, frame)
        return frame

    # Download tickers from yahoo finance
    def download_days_yahoo(self, ticker: str) -> pandas.DataFrame:
        if ticker in self.EXCLUDE_TICKERS:
            return self.empty_frame

        # Check if data exists
        ticker = str(ticker).upper()
        file = self._get_data_file(self.TF_DAILY, ticker)
        if os.path.isfile(file):
            return self.load_csv(self.TF_DAILY, ticker)

        if self.data_disable_download:
            return self.empty_frame

        try:
            # Get parsed data
            self.log_info('Downloading yahoo daily data for %s' % ticker, category=self.LOG_DOWNLOAD)
            yf = YahooFinancials(ticker)
            data = yf.get_historical_price_data(
                self.d2str(datetime.datetime(2000, 1, 1)),
                self.d2str(datetime.datetime.now()),
                'daily')
        except Exception as err:
            self.log_error('Unable to read data for %s: %s' % (ticker, err), category=self.LOG_DOWNLOAD_ERROR)
            return self.empty_frame

        # Return empty dataset
        if data.get(ticker) is None or data[ticker].get('prices') is None or \
                data[ticker].get('timeZone') is None or len(data[ticker]['prices']) == 0:
            self.log_warn('Yahoo: no data for %s' % ticker, category=self.LOG_DOWNLOAD_ERROR)
            return self.empty_frame

        prices = {}
        tz = datetime.timezone(datetime.timedelta(seconds=data[ticker]['timeZone']['gmtOffset']))
        for rec in sorted(data[ticker]['prices'], key=lambda r: r['date']):
            if rec.get('type') is None:
                rec_date = datetime.datetime.fromtimestamp(rec['date'], tz=tz)
                date = datetime.datetime(rec_date.year, rec_date.month, rec_date.day)
                self._check_add_price(
                    prices, ticker, date,
                    rec['open'], rec['high'], rec['low'], rec['close'], rec['volume'])

        frame = self._make_data_frame(prices)
        self.save_csv(self.TF_DAILY, ticker, frame)
        return frame

    # Download tickers from google finance
    def download_days_google(self, ticker: str) -> pandas.DataFrame:
        if ticker in self.EXCLUDE_TICKERS:
            return self.empty_frame

        # Check if data exists
        ticker = str(ticker).upper()
        file = self._get_data_file(self.TF_DAILY, ticker)
        if os.path.isfile(file):
            return self.load_csv(self.TF_DAILY, ticker)

        if self.data_disable_download:
            return self.empty_frame

        # Download parameters
        param = {
            'q': ticker,  # Stock symbol (ex: "AAPL")
            'i': "86400",  # Interval size in seconds ("86400" = 1 day intervals)
            'p': "20Y"  # Period (Ex: "1Y" = 1 year)
        }

        try:
            # Get price data (return pandas dataframe)
            self.log_info('Downloading google daily data for %s' % ticker, category=self.LOG_DOWNLOAD)
            data = googlefinance.client.get_price_data(param)
        except Exception as err:
            self.log_error('Unable to read data for %s: %s' % (ticker, err), category=self.LOG_DOWNLOAD_ERROR)
            return self.empty_frame

        if data.empty:
            self.log_warn('Google: no data for %s' % ticker, category=self.LOG_DOWNLOAD_ERROR)
            return self.empty_frame

        prices = {}
        for index, rec in data.iterrows():
            date = datetime.datetime(index.year, index.month, index.day)
            self._check_add_price(prices, ticker, date,
                                  rec['Open'], rec['High'], rec['Low'], rec['Close'], rec['Volume'])

        frame = self._make_data_frame(prices)
        self.save_csv(self.TF_DAILY, ticker, frame)
        return frame

    # Download data tickers
    def download_all(self, tickers: list):
        pool = multiprocessing.Pool(1 if self.data_source == self.DATA_ALPHA else 16)
        if self.data_source == Strategy.DATA_YAHOO:
            pool.map(self.download_days_yahoo, tickers)
        elif self.data_source == Strategy.DATA_GOOGLE:
            pool.map(self.download_days_google, tickers)
        elif self.data_source == Strategy.DATA_ALPHA:
            pool.map(self.download_days_alpha, tickers)
        else:
            self.log_fatal('Unknown data provider: %s' % self.data_source)

    # Run backest
    def run(self, report: bool = True):
        # Load ticker cache
        self.load_hyper_cache()

        # Load events
        if self.event_source is not None:
            self.load_events_cache(self.EVENT_FILE[self.event_source])

        # Iterate days
        while self.next_day():
            self.on_day()

        # Finish
        self.on_finish()
        self.generate_report()

        # Print report if needed
        if report:
            self.print_report()

        # Flush log file
        if self.log_file_obj:
            self.log_file_obj.flush()

        return self

    # Handle day
    def on_day(self):
        pass

    # Backtesting finished
    def on_finish(self):
        pass

    # Get all trades
    @property
    def all_trades(self) -> list:
        return self.day_trades + self.overnight_trades + self.current_overnight_trades

    # Get overnight BP
    @property
    def overnight_bp(self) -> float:
        return self.balance * self.overnight_margin - self.margin_used

    # Get day BP
    @property
    def day_bp(self) -> float:
        return self.balance * self.day_margin - self.margin_used

    # Get margin used
    @property
    def margin_used(self):
        return sum(t.margin_used for t in self.all_trades)

    # Get count of long positions
    @property
    def active_longs(self) -> int:
        return sum(t.direction == Trade.LONG for t in self.all_trades)

    # Get count of short positions
    @property
    def active_shorts(self) -> int:
        return sum(t.direction == Trade.SHORT for t in self.all_trades)

    # Shift to next trading day
    def next_day(self) -> bool:
        # Starting new round
        if self.date_current is None:
            # Set start date to working day
            self.date_current = self.date_start
            if self.is_holiday(self.date_start):
                self.date_current = self.get_next_trade_day(self.date_start)

            self.balance = self.balance_start
            self.gross_balance = self.balance_start

            self.balance_series.at[self.date_current] = self.balance
            self.drawdown_series.at[self.date_current] = 0
        else:
            self.finish_day()
            self.date_current = self.get_next_trade_day(self.date_current)

        # Check finish date
        if self.date_current > self.date_end:
            return False

        # Start new day

        self.log_info(
            '****** %s - DAY STARTED (BP day %.2f, night %.2f, balance %.2f, prev longs %d, prev shorts %d)'
            % (self.d2str(self.date_current), self.day_bp, self.overnight_bp, self.balance,
               self.active_longs, self.active_shorts))

        self.current_overnight_trades = []
        self.day_start_balance = self.balance
        return True

    # Remove trade
    def _remove_trade(self, trade: Trade):
        if trade in self.overnight_trades:
            self.overnight_trades.remove(trade)
        elif trade in self.current_overnight_trades:
            self.current_overnight_trades.remove(trade)
        elif trade in self.day_trades:
            self.day_trades.remove(trade)

    # Fix losing trade
    def _fix_stop_trade(self, result: float, commission: float, volume: int):
        self.commission_total += commission
        self.balance -= commission
        self.balance -= result
        self.total_volume += volume
        self.reached_stop_count += 1
        self.slippage_total += self.slippage * volume
        self.gross_balance -= result
        self.total_losings += result

    # Fix close trade
    def _fix_close_trade(self, result: float, commission: float, volume: int, direction: str):
        self.commission_total += commission
        self.balance -= commission
        self.balance += result
        self.gross_balance += result
        self.total_volume += volume

        if direction == Trade.LONG:
            self.long_wins += result >= 0
        else:
            self.short_wins += result >= 0

        if result >= 0:
            self.total_winnings += result
        else:
            self.total_losings += abs(result)

    # Finish trade day
    def finish_day(self):
        # Close overnight stops
        for trade in self.overnight_trades:
            # Get current day bar
            bar = self.get_day_data(trade.ticker, self.date_current)
            if not bar:
                self._remove_trade(trade)
                self.log_error(
                    'Removed overnight trade #%d for %s - no bar data for day %s' %
                    (trade.id, trade.ticker, self.d2str(self.date_current)),
                    category=self.LOG_NO_BAR_DATA)
                continue

            # Check for long stops
            if trade.stop and trade.direction == Trade.LONG and (trade.price - bar.Open) > trade.stop:
                slippage = self.slippage if trade.stop < trade.price else 0
                comm = self.get_comission(trade.volume, bar.Open)
                result = (trade.price - bar.Open + slippage) * trade.volume
                self._fix_stop_trade(result=result, commission=comm, volume=trade.volume)
                self._remove_trade(trade)
                self.log_sell(
                    'Close overnight STOP %s #%d at %.2f, result=%.2f, comm=%.2f, vol=%d, balance=%.2f' %
                    (trade.ticker, trade.id, bar.Open, -result, comm, trade.volume, self.balance),
                    category=self.LOG_TRADE)

            # Check for short stops
            if trade.stop and trade.direction == Trade.SHORT and (bar.Open - trade.price) > trade.stop:
                slippage = self.slippage if trade.stop < trade.price else 0
                comm = self.get_comission(trade.volume, bar.Open)
                result = (bar.Open - trade.price + slippage) * trade.volume
                self._fix_stop_trade(result=result, commission=comm, volume=trade.volume)
                self._remove_trade(trade)
                self.log_cover(
                    'Close overnight STOP %s #%d at %.2f, result=%.2f, comm=%.2f, vol=%d, balance=%.2f' %
                    (trade.ticker, trade.id, bar.Open, -result, comm, trade.volume, self.balance),
                    category=self.LOG_TRADE)

        # Calculate stops for current trades
        for trade in self.all_trades:
            # Get current day bar
            bar = self.get_day_data(trade.ticker, self.date_current)
            if not bar:
                self._remove_trade(trade)
                self.log_error(
                    'Removed trade #%d for %s - no bar data for day %s' %
                    (trade.id, trade.ticker, self.d2str(self.date_current)),
                    category=self.LOG_NO_BAR_DATA)
                continue

            # Check for long stops
            if trade.stop and trade.direction == Trade.LONG and (trade.price - bar.Low) > trade.stop:
                slippage = self.slippage if trade.stop < trade.price else 0
                comm = self.get_comission(trade.volume, trade.price - trade.stop)
                result = (trade.stop + slippage) * trade.volume
                self._fix_stop_trade(result=result, commission=comm, volume=trade.volume)
                self._remove_trade(trade)
                self.log_sell(
                    'Close %sSTOP %s #%d at %.2f, result=%.2f, comm=%.2f, vol=%d, balance=%.2f' %
                    (trade.type_str, trade.ticker, trade.id, trade.price - trade.stop - slippage, -result, comm,
                     trade.volume, self.balance),
                    category=self.LOG_TRADE)

            # Check for short stops
            if trade.stop and trade.direction == Trade.SHORT and (bar.High - trade.price) > trade.stop:
                slippage = self.slippage if trade.stop < trade.price else 0
                comm = self.get_comission(trade.volume, bar.Open + trade.stop)
                result = (trade.stop + slippage) * trade.volume
                self._fix_stop_trade(result=result, commission=comm, volume=trade.volume)
                self._remove_trade(trade)
                self.log_cover(
                    'Close %sSTOP %s #%d at %.2f, result=%.2f, comm=%.2f, vol=%d, balance=%.2f' %
                    (trade.type_str, trade.ticker, trade.id, trade.price + trade.stop + slippage, -result, comm,
                     trade.volume, self.balance),
                    category=self.LOG_TRADE)

        # Close trades in current day
        for trade in self.all_trades:
            if trade.exit_date != self.date_current:
                continue

            # Get current day bar
            bar = self.get_day_data(trade.ticker, self.date_current)
            if not bar:
                self._remove_trade(trade)
                self.log_error(
                    'Removed trade #%d for %s - no bar data for day %s' %
                    (trade.ticker, trade.id, self.d2str(self.date_current)),
                    category=self.LOG_NO_BAR_DATA)
                continue

            # Exit longs
            if trade.direction == Trade.LONG:
                comm = self.get_comission(trade.volume, bar.Close)
                result = (bar.Close - trade.price) * trade.volume
                self._fix_close_trade(result=result, commission=comm, volume=trade.volume, direction=Trade.LONG)
                self._remove_trade(trade)
                self.log_sell(
                    'Close %s%s #%d at %.2f, result=%.2f, comm=%.2f, vol=%d, balance=%.2f' %
                    (trade.type_str, trade.ticker, trade.id, bar.Close, result, comm, trade.volume, self.balance),
                    category=self.LOG_TRADE)

            # Exit shorts
            if trade.direction == Trade.SHORT:
                comm = self.get_comission(trade.volume, bar.Close)
                result = (trade.price - bar.Close) * trade.volume
                self._fix_close_trade(result=result, commission=comm, volume=trade.volume, direction=Trade.SHORT)
                self._remove_trade(trade)
                self.log_cover(
                    'Close %s%s #%d at %.2f, result=%.2f, comm=%.2f, vol=%d, balance=%.2f' %
                    (trade.type_str, trade.ticker, trade.id, bar.Close, result, comm, trade.volume, self.balance),
                    category=self.LOG_TRADE)

        # Day results
        day_result = self.balance - self.day_start_balance

        if day_result > 0:
            self.winning_streak += 1
            self.max_losing_streak = max(self.max_losing_streak, self.losing_streak)
            self.losing_streak = 0
        else:
            self.losing_streak += 1
            self.max_winning_streak = max(self.max_winning_streak, self.winning_streak)
            self.winning_streak = 0

        # Calculate drawdown
        self.balance_high = max(self.balance_high, self.balance)
        self.drawdown_series.at[self.date_current] = (1 - self.balance / self.balance_high) * 100

        # Fix balance
        self.overnight_trades += self.current_overnight_trades
        self.current_overnight_trades = []
        self.balance_series.at[self.date_current] = self.balance

        # log
        self.log_info(
            '****** %s - DAY FINISHED (balance %.2f, opened longs %d, opened shorts %d, day result = %.2f)'
            % (self.d2str(self.date_current), self.balance, self.active_longs, self.active_shorts,
               day_result))

    # Add open to close trade
    def add_open_close_trade(self,
                             ticker: str,
                             entry_date: datetime,
                             exit_date: datetime,
                             direction: str,
                             price: float,
                             stop: float,
                             volume: int,
                             pos_risk: float):

        # Check current day
        if entry_date != self.date_current:
            self.log_error('Entry date %s not match current day' % entry_date, category=self.LOG_TRADE_ERROR)
            return

        # Create record
        self.total_trades += 1
        rec = Trade(
            trade_id=self.total_trades,
            ticker=ticker,
            entry_date=entry_date,
            exit_date=exit_date,
            direction=direction,
            price=price,
            stop=stop,
            volume=volume,
            margin_used=price * volume,
            pos_risk=pos_risk)

        if exit_date != self.date_current:
            self.current_overnight_trades.append(rec)
        else:
            self.day_trades.append(rec)

        # Adjust counters
        comm = self.get_comission(volume, price)
        self.balance -= comm
        self.commission_total += comm
        self.total_volume += rec.volume

        # Log
        if rec.direction == Trade.LONG:
            self.long_trades += 1
            self.log_buy(
                'Open %strade #%d BUY %s at %.2f, stop=%.2f, comm=%.2f, vol=%d, risk=%.2f' %
                (rec.type_str, rec.id, rec.ticker, price, stop, comm, volume, pos_risk), category=self.LOG_TRADE)
        else:
            self.short_trades += 1
            self.log_short(
                'Open %strade #%d SELL %s at %.2f, stop=%.2f, comm=%.2f, vol=%d, risk-%.2f' %
                (rec.type_str, rec.id, rec.ticker, price, stop, comm, volume, pos_risk), category=self.LOG_TRADE)

    # Check report time
    def _check_report_time(self, ticker: str, report_date: datetime, report_time: str) -> bool:
        # Check reports field
        if report_time not in ['BMO', 'AMC']:
            self.log_error(
                'Invalid event time for %s at %s - %s' % (ticker, self.d2str(report_date), report_time),
                category=self.LOG_EVENT_ERROR)
            return False
        else:
            return True

    # Check if bar exist in specified date
    def _is_bar_exist(self, date: datetime, ticker: str, logging: bool = True) -> bool:
        bars = self.get_daily_data(ticker)
        if date not in bars.index:
            self.missed_bar_count += 1
            if logging:
                self.log_error('No bar data for %s at %s' % (ticker, self.d2str(date)), category=self.LOG_NO_BAR_DATA)
            return False

        if bars.loc[date]['Error']:
            self.error_bar_count += 1
            if logging:
                self.log_error(
                    'Error bar data for %s at %s rejected' % (ticker, self.d2str(date)),
                    category=self.LOG_ERROR_BAR_DATA)
            return False

        return True

    # Validate event
    def _validate_event(self, event: Event) -> bool:
        # Check for holiday
        off_day = self.is_holiday(event.date)
        if off_day is not None:
            self.holiday_event_count += 1
            self.log_error(
                'Holiday event detected for %s at %s - %s' %
                (event.ticker, self.d2str(event.date), off_day), category=self.LOG_EVENT_HOLIDAY)
            return False

        # Discover entry date
        entry_date = event.date
        if event.reports == 'BMO':
            if not self._is_bar_exist(entry_date, event.ticker):
                return False
        else:
            entry_date = self.get_next_trade_day(entry_date)
            if not self._is_bar_exist(entry_date, event.ticker):
                return False

        # Check next date
        next_date = self.get_next_trade_day(entry_date)
        if not self._is_bar_exist(next_date, event.ticker, False):
            next_date = None

        # Add ticker for date
        self.valid_events_count += 1
        event.entry_date = entry_date
        event.next_date = next_date
        return True

    # Load estimize events
    def get_estimize_events(self, file: str):
        tickers, events, = {}, {}
        for index, row in pandas.read_excel(file).iterrows():
            # Add unique ticker
            ticker = str(row['ticker']).upper()
            tickers[ticker] = 1

            # Get report date
            report_date = row['date'].to_pydatetime()

            # Check reports field
            if not self._check_report_time(ticker, report_date, row['reports']):
                continue

            # Create event
            event = Event(
                ticker=ticker,
                date=report_date,
                reports=row['reports'],
                report_type=Event.EPS_AND_REV,
                eps_con=row['epsWallStreet'],
                eps_act=row['epsActual'],
                rev_con=row['revWallStreet'],
                rev_act=row['revActual'])

            # Check data
            if not event.is_valid:
                self.log_error(
                    'Invalid or missing event data for %s at %s' % (ticker, self.d2str(report_date)),
                    category=self.LOG_EVENT_ERROR)
                continue

            # Validate event
            if not self._validate_event(event):
                continue

            # Add ticker for date
            events.setdefault(event.entry_date, []).append(event)

        return list(tickers.keys()), events

    # Load estimize final with TOS check
    def get_estimize_final_events(self, file: str):
        tickers, events, = {}, {}
        for index, row in pandas.read_csv(file).iterrows():
            # Add unique ticker
            ticker = str(row['ticker']).upper()
            tickers[ticker] = 1

            # Calculate report date
            report_date = datetime.datetime.strptime(row['date'], '%Y-%m-%d')

            # Check reports field
            if not self._check_report_time(ticker, report_date, row['reports']):
                continue

            # Create event
            event = Event(
                ticker=ticker,
                date=report_date,
                reports=row['reports'],
                report_type=Event.EPS_AND_REV,
                eps_con=row['epsWallStreet'],
                eps_act=row['epsActual'],
                rev_con=row['revWallStreet'],
                rev_act=row['revActual'])

            # Check data
            if not event.is_valid:
                self.log_error(
                    'Invalid or missing event data for %s at %s' % (ticker, self.d2str(report_date)),
                    category=self.LOG_EVENT_ERROR)
                continue

            # Validate event
            if not self._validate_event(event):
                continue

            # Add ticker for date
            events.setdefault(event.entry_date, []).append(event)

        return list(tickers.keys()), events

    # Load IB events
    def get_ib_events(self, file: str):
        tickers, events, = {}, {}
        for index, row in pandas.read_excel(file).iterrows():
            # Add unique ticker
            ticker = str(row['ticker']).upper()
            tickers[ticker] = 1

            # Calculate report date
            report_date = row['date'].to_pydatetime()

            # Check reports field
            if not self._check_report_time(ticker, report_date, row['reports']):
                continue

            # Create event
            event = Event(
                ticker=ticker,
                date=report_date,
                reports=row['reports'],
                report_type=Event.EPS_AND_REV,
                eps_con=row['eps_con'],
                eps_act=row['eps_act'],
                rev_con=row['rev_con'],
                rev_act=row['rev_act'])

            # Check data
            if not event.is_valid:
                self.log_error(
                    'Invalid or missing event data for %s at %s' % (ticker, self.d2str(report_date)),
                    category=self.LOG_EVENT_ERROR)
                continue

            # Validate event
            if not self._validate_event(event):
                continue

            # Add ticker for date
            events.setdefault(event.entry_date, []).append(event)

        return list(tickers.keys()), events

    # Load TOS events
    def get_tos_events(self, file: str):
        tickers, events, = {}, {}
        for index, row in pandas.read_excel(file).iterrows():
            # Add unique ticker
            ticker = str(row['ticker']).upper()
            tickers[ticker] = 1

            # Calculate report date
            report_date = row['date'].to_pydatetime()

            # Check reports field
            if not self._check_report_time(ticker, report_date, row['reports']):
                continue

            # Create event
            event = Event(
                ticker=ticker,
                date=report_date,
                reports=row['reports'],
                report_type=Event.EPS_ONLY,
                eps_con=row['eps_con'],
                eps_act=row['eps_act'])

            # Check data
            if not event.is_valid:
                self.log_error(
                    'Invalid or missing event data for %s at %s' % (ticker, self.d2str(report_date)),
                    category=self.LOG_EVENT_ERROR)
                continue

            # Validate event
            if not self._validate_event(event):
                continue

            # Add ticker for date
            events.setdefault(event.entry_date, []).append(event)

        return list(tickers.keys()), events

    # Load Zacks events
    def get_zacks_events(self, file: str):
        tickers, events, = {}, {}
        for index, row in pandas.read_excel(file).iterrows():
            # Add unique ticker
            ticker = str(row['ticker']).upper()
            tickers[ticker] = 1

            # Calculate report date
            report_date = row['date'].to_pydatetime()

            # Check reports field
            if not self._check_report_time(ticker, report_date, row['reports']):
                continue

            # Create event
            event = Event(
                ticker=ticker,
                date=report_date,
                reports=row['reports'],
                report_type=Event.EPS_AND_REV,
                eps_con=row['epsEst'],
                eps_act=row['epsAct'],
                rev_con=row['revEst'],
                rev_act=row['revAct'])

            # Check data
            if not event.is_valid:
                self.log_error(
                    'Invalid or missing event data for %s at %s' % (ticker, self.d2str(report_date)),
                    category=self.LOG_EVENT_ERROR)
                continue

            # Validate event
            if not self._validate_event(event):
                continue

            # Add ticker for date
            events.setdefault(event.entry_date, []).append(event)

        return list(tickers.keys()), events

    # Load Portfolio 123 events
    def get_portfolio123_events(self, file: str):
        tickers, events, = {}, {}
        for index, row in pandas.read_excel(file).iterrows():
            # Add unique ticker
            ticker = str(row['Ticker']).upper()
            ticker = re.sub('\^.*$', '', ticker)
            ticker = re.sub('^\d+', '', ticker)
            if '.' in ticker:
                continue

            tickers[ticker] = 1

            # Calculate report date
            report_date = row['@date_'].to_pydatetime()

            # Get bars
            bar0 = self.get_day_data(ticker, report_date)
            bar1 = self.get_day_data(ticker, self.get_next_trade_day(report_date))

            # Skip if no bar data for current and next working daysEVENTS_PORTFOLIO_123
            if bar0 is None or bar1 is None:
                continue

            # Compare todays and next day volume, if todays > next - BMO
            reports = 'BMO' if bar0.Volume > bar1.Volume else 'AMC'

            # Create event
            event = Event(
                ticker=ticker,
                date=report_date,
                reports=reports,
                report_type=Event.EPS_AND_REV,
                eps_con=row['@est_eps'],
                eps_act=row['@act_eps'],
                rev_con=row['@est_sales'],
                rev_act=row['@act_sales'])

            # Check data
            if not event.is_valid:
                self.log_error(
                    'Invalid or missing event data for %s at %s' % (ticker, self.d2str(report_date)),
                    category=self.LOG_EVENT_ERROR)
                continue

            # Validate event
            if not self._validate_event(event):
                continue

            # Add ticker for date
            events.setdefault(event.entry_date, []).append(event)

        return list(tickers.keys()), events

    # Load estimize filtered with TOS events
    def get_estimize_tos_events(self, file: str):
        return self.get_estimize_events(file)

    # Calculate hash of file info
    @staticmethod
    def _hash_file(file: str, salt: str = None):
        m = hashlib.md5()
        info = os.stat(file)
        info_str = '%d, %f' % (info.st_size, info.st_mtime)
        m.update(info_str.encode())
        if salt:
            m.update(salt.encode())

        return m.hexdigest()

    # Preload events cache
    def load_events_cache(self, file: str):
        cur_hash = self._hash_file(file, self.data_source)
        path = os.path.join(self.data_base_dir, '%s_events_cache.dat' % self.event_source)

        # Try to load events and compare hash
        if os.path.exists(path):
            self.log_info('Loading events from cache %s' % path, category=self.LOG_EVENT)
            with open(path, 'rb') as handle:
                data = pickle.load(handle)
                if data is not None and data.get('hash') == cur_hash and data.get('events') is not None:
                    self.events = data['events']
                    self.holiday_event_count = data['holiday_event_count']
                    self.missed_bar_count = data['missed_bar_count']
                    self.error_bar_count = data['error_bar_count']
                    self.valid_events_count = data['valid_events_count']
                    return

        # Load events from file
        self.log_info('Events cache not exits or outdated - creating new from %s' % file, category=self.LOG_EVENT)
        _, self.events = self.get_events()

        # Save events cache
        rec = {
            'hash': cur_hash,
            'events': self.events,
            'holiday_event_count': self.holiday_event_count,
            'missed_bar_count': self.missed_bar_count,
            'error_bar_count': self.error_bar_count,
            'valid_events_count': self.valid_events_count
        }

        self.log_info('Saving updated events cache to %s' % path)
        with open(path, 'wb') as handle:
            pickle.dump(rec, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # Get events data
    def get_events(self):
        tickers, events = [], {}

        if self.event_source == self.EVENTS_ESTIMIZE:
            tickers, events = self.get_estimize_events(self.EVENT_FILE[self.EVENTS_ESTIMIZE])
        elif self.event_source == self.EVENTS_TOS:
            tickers, events = self.get_tos_events(self.EVENT_FILE[self.EVENTS_TOS])
        elif self.event_source == self.EVENTS_ZACKS:
            tickers, events = self.get_zacks_events(self.EVENT_FILE[self.EVENTS_ZACKS])
        elif self.event_source == self.EVENTS_ESTIMIZE_TOS:
            tickers, events = self.get_estimize_tos_events(self.EVENT_FILE[self.EVENTS_ESTIMIZE_TOS])
        elif self.event_source == self.EVENTS_ESTIMIZE_FINAL:
            tickers, events = self.get_estimize_final_events(self.EVENT_FILE[self.EVENTS_ESTIMIZE_FINAL])
        elif self.event_source == self.EVENTS_IB:
            tickers, events = self.get_ib_events(self.EVENT_FILE[self.EVENTS_IB])
        elif self.event_source == self.EVENTS_PORTFOLIO_123:
            tickers, events = self.get_portfolio123_events(self.EVENT_FILE[self.EVENTS_PORTFOLIO_123])
        else:
            self.log_fatal('Unsuppored event source %s' % self.event_source)

        return tickers, events

    # Calculate maximum position size for money accounting comission
    def get_volume_comm_adjusted(self, money: float, price: float) -> int:
        volume = int(money // price)
        while (volume * price) + self.get_comission(volume, price) > money:
            volume -= 1

        return volume

    # Generate report
    def generate_report(self):
        total_wins = self.long_wins + self.short_wins
        net_balance = self.gross_balance - self.commission_total - self.slippage_total
        year_count = max(1, self.date_end.year - self.date_start.year)
        year_yield = (math.pow(net_balance / self.balance_start, 1 / year_count) - 1) * 100

        # Max drawdown
        drawdown = self.drawdown_series.max()

        # Collect stats
        self.tprint()
        self.tprint('**** Backtest statistics')
        self.tprint('Data feed', self.data_source)
        self.tprint('Events feed', self.event_source)
        self.tprint('Date range', '%s..%s' % (self.d2str(self.date_start), self.d2str(self.date_end)))
        self.tprint('Broker profile', self.broker)
        self.tprint('Intraday/overnight margins', '%d/%d' % (self.day_margin, self.overnight_margin))
        self.tprint('Beginning balance', '$%.2f' % self.balance_start)
        self.tprint('Ending balance (Gross)', '$%.2f' % self.gross_balance)
        self.tprint('Ending balance (Net)', '$%.2f' % net_balance)
        self.tprint('Trade volume', '%.0f' % self.total_volume)
        self.tprint('Comissions paid', '$%.2f' % self.commission_total)
        self.tprint('Slippage paid', '$%.2f' % self.slippage_total)
        self.tprint('Maximum drawdown', '%.2f%%' % drawdown)
        self.tprint('Total trades', '%d (%d long, %d short)' % (self.total_trades, self.long_trades, self.short_trades))
        self.tprint('Winning percentage', '%.2f%% (%.2f%% long, %.2f%% short)' %
                    (self._safe_div(total_wins * 100, self.total_trades),
                     self._safe_div(self.long_wins * 100, total_wins),
                     self._safe_div(self.short_wins * 100, total_wins)))
        self.tprint('Stops reached', '%d' % self.reached_stop_count)
        self.tprint('Averge profit per trade', '$%.2f' % self._safe_div(self.total_winnings, total_wins))
        self.tprint('Averge loss per trade',
                    '-$%.2f' % (self._safe_div(self.total_losings, self.total_trades - total_wins)))
        self.tprint('Largest winning streak, days', '%d' % self.max_winning_streak)
        self.tprint('Largest losing streak, days', '%d' % self.max_losing_streak)
        self.tprint('Profit factor', '%.2f' % (self._safe_div(self.total_winnings, self.total_losings)))
        self.tprint('Y/y yield', '%.2f%%' % year_yield)
        self.tprint('Holiday events detected', '%d' % self.holiday_event_count)
        self.tprint('Bars missed', '%d' % self.missed_bar_count)
        self.tprint('Error bars skipped', '%d' % self.error_bar_count)
        self.tprint('Valid events count', '%d' % self.valid_events_count)
        self.tprint('No day/overnight margins', '%d/%d' % (self.no_day_margin_count, self.no_overnight_margin_count))

        # Merge user info
        if len(self.user_lines) > 0:
            self.tprint()
            self.tprint('**** Additional info')
            for line in self.user_lines:
                self.tprint(line['header'], line['value'])

    # Print report
    def print_report(self):
        # Print
        for item in self.out_lines:
            header, value = item.get('header'), item.get('value')
            if header and value:
                self._log_file(self._pad(header) + ' ' + value)
                print(self._pad(header) + ' ' + value)
            elif header:
                print(header)
                self._log_file(header)
            else:
                self._log_file('')
                print()

        # Blank line
        self._log_file('')
        print()

    # Plot result
    def plot_result(self, drawdown: bool = True):
        # Create report frame
        names, values = [], []
        for line in self.out_lines:
            if line['header'] and not line['header'].startswith('*'):
                names.append(line['header'])
                values.append(line['value'] or '')

        # Add even cell
        if len(names) % 2 != 0:
            names.append('')
            values.append('')

        half = len(names) // 2
        table = pandas.DataFrame({
            'h1': pandas.Series(names[:half]),
            'v1': pandas.Series(values[:half]),
            'h2': pandas.Series(names[half:]),
            'v2': pandas.Series(values[half:]),
        })

        fig = plt.figure(**self.PLOT_SIZE)
        ax1 = fig.add_subplot(2, 1, 1)
        ax1.plot(self.balance_series)
        ax1.set_ylabel('balance, $')

        title = '%s' % self.name
        plt.title(title)

        if drawdown:
            ax2 = ax1.twinx()
            ax2.plot(self.drawdown_series, 'r:')
            ax2.set_ylabel('drawdown, %', color='r')
            for tl in ax2.get_yticklabels():
                tl.set_color('r')

        # Add table
        tx1 = fig.add_subplot(2, 1, 2, frameon=False)
        tx1.axis("off")
        tx1.table(cellText=table.values, loc='center')

        plt.show()
        return self

    # Compare balances
    @staticmethod
    def compare_balances(st_list: list, drawdown: bool = False):
        fig = plt.figure(**Strategy.PLOT_SIZE)
        ax1 = fig.add_subplot(111)
        ax1.set_ylabel('balance, $')

        ax2 = None
        if drawdown:
            ax2 = ax1.twinx()
            ax2.set_ylabel('drawdown, %', color='r')
            for tl in ax2.get_yticklabels():
                tl.set_color('r')

        for st in st_list:
            p = ax1.plot(st.balance_series, label=st.name, linewidth=2)
            if drawdown:
                ax2.plot(st.drawdown_series, color=p[0].get_color(), linestyle='dashed', linewidth=1,
                         label='%s drawdown' % st.name)

        # Show main legend
        ax1.legend(loc=2)

        # Show drawdown legend
        if drawdown:
            ax2.legend(loc=1)

        # Show plot
        plt.show()

    # Dump events to file
    def dump_events(self, file: str):
        res = []
        for entry_date in self.events.keys():
            for event in self.events[entry_date]:
                res.append(dotmap.DotMap({
                    'date': event.date,
                    'ticker': event.ticker,
                    'reports': event.reports,
                    'epsEst': event.eps_con,
                    'epsAct': event.eps_act,
                    'revEst': event.rev_con,
                    'revAct': event.rev_act
                }))

        with open(file, 'w') as f:
            f.write('date,ticker,reports,epsEst,epsAct,revEst,revAct\n')
            for event in sorted(res, key=lambda e: e.date.timestamp()):
                f.write('%s,%s,%s,%.2f,%.2f,%.2f,%.2f\n' % (self.d2str(event.date), event.ticker, event.reports,
                        event.epsEst, event.epsAct, event.revEst, event.revAct))

