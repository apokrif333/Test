import cmath
import datetime

import pandas

import strategy


# Strategy
class Earnings(strategy.Strategy):
    # Field list
    __slots__ = [
        'price_range',
        'portfolio_size',
        'portfolio_risk',
        'min_avg_volume',
        'small_avg_volume_skipped',
        'max_volume',
        'long_same_day'
    ]

    # Constructor
    def __init__(
            self,
            name='Earnings',
            start_balance: int = 10000,
            price_range: tuple = (5, 100),
            min_avg_volume: int = 0,
            portfolio_size: int = 20,
            portfolio_risk: float = 1,
            max_volume: int = 15000,
            disable_download: bool = True,
            long_same_day: bool = True,
            data_source: str = strategy.Strategy.DATA_YAHOO,
            event_source: str = strategy.Strategy.EVENTS_PORTFOLIO_123,
            date_start=datetime.datetime(2012, 1, 1),
            date_end=datetime.datetime(2018, 9, 1)):

        strategy.Strategy.__init__(
            self,
            name=name,
            data_source=data_source,
            event_source=event_source,
            broker=strategy.Strategy.BROKER_IB_CFD_STRICT,
            day_margin=4,
            overnight_margin=4,
            disable_download=disable_download,
            date_start=date_start,
            date_end=date_end,
            balance_start=start_balance)

        # Strategy parameters
        self.price_range = price_range
        self.min_avg_volume = min_avg_volume
        self.small_avg_volume_skipped = 0
        self.portfolio_size = portfolio_size
        self.portfolio_risk = portfolio_risk
        self.max_volume = max_volume
        self.long_same_day = long_same_day

    # Preprocess history
    def history_preprocess(self, data: pandas.DataFrame):
        data['atr'] = (data['High'] - data['Low']).rolling(10).mean()
        data['vol_avg'] = data['Volume'].rolling(20).mean()

    # Rank events
    def rank_events(self, report_list: list) -> (list, list):
        for e in report_list:
            if e.type == strategy.Event.EPS_ONLY:
                if (e.eps_con and e.eps_act) != 0:
                    #
                    if e.eps_change > 0 and (self.long_same_day or e.next_date is not None):
                        e.attrs.rank = e.eps_change + 1
                        e.attrs.pos = strategy.Trade.LONG
                    #
                    elif e.eps_change < 0:
                        e.attrs.rank = e.eps_change - 1
                        e.attrs.pos = strategy.Trade.SHORT
                    #
                    else:
                        e.attrs.rank = 0
                        e.attrs.pos = None
                else:
                    e.attrs.rank = 0
            elif e.type == strategy.Event.EPS_AND_REV:
                if (e.eps_con and e.eps_act and e.rev_con and e.rev_act) != 0:
                    #
                    if e.eps_change > 0 and e.rev_change > 0 and (self.long_same_day or e.next_date is not None):
                        e.attrs.rank = (e.eps_change + 1) * (e.rev_change + 1)
                        e.attrs.pos = strategy.Trade.LONG
                    #
                    elif e.eps_change < 0 and e.rev_change < 0:
                        e.attrs.rank = - abs((e.eps_change - 1) * (e.rev_change - 1))
                        e.attrs.pos = strategy.Trade.SHORT
                    #
                    else:
                        e.attrs.rank = 0
                        e.attrs.pos = None
                else:
                    e.attrs.rank = 0
                    e.attrs.pos = None

        longs = sorted([e for e in report_list if e.attrs.rank > 0], key=lambda v: v.attrs.rank, reverse=True)
        shorts = sorted([e for e in report_list if e.attrs.rank < 0], key=lambda v: v.attrs.rank)
        return longs, shorts

    # Filter price range
    def filter_price(self, events: list) -> list:
        result = []
        for event in events:
            bar = self.get_day_data(event.ticker, self.date_current)
            if self.price_range[0] <= bar.Open <= self.price_range[1]:
                result.append(event)
            else:
                self.log_warn('Skipped event for %s, price not in range %.2f' % (event.ticker, bar.Open))

        return result

    # Filter average volume
    def filter_volume(self, events: list) -> list:
        result = []
        for event in events:
            bar = self.get_day_data(event.ticker, self.date_current)
            if cmath.isnan(bar.vol_avg):
                self.log_warn('Skipped event for %s, no volume data' % event.ticker)
                continue

            if bar.vol_avg < self.min_avg_volume:
                self.small_avg_volume_skipped += 1
                self.log_warn(
                    'Skipped event for %s, average volume %d < %d' %
                    (event.ticker, bar.vol_avg, self.min_avg_volume))
                continue

            result.append(event)

        return result

    # Filter CFD
    def filter_cfd(self, events: list) -> list:
        if self.broker == strategy.Strategy.BROKER_IB_CFD:
            return [e for e in events if self.is_stock_available(e.ticker)]
        else:
            return events

    # Process one day
    def on_day(self):
        # Get reports for day
        report_list = self.events.get(self.date_current)
        if not report_list:
            return

        # Rank events
        long, short = self.rank_events(report_list)

        # Filter events
        long = self.filter_volume(self.filter_price(self.filter_cfd(long)))
        short = self.filter_volume(self.filter_price(self.filter_cfd(short)))

        # Basket percentage
        p_long = 0
        c_long, c_short = (len(long), len(short))

        if c_long + c_short > 0:
            p_long = c_long / (c_long + c_short)

        # Create list of events to enter
        pos_left = self.portfolio_size - self.active_longs - self.active_shorts
        if c_long + c_short <= pos_left:
            pos_list = long + short
        else:
            pos_list = long[:int(pos_left * p_long)] + short[:pos_left - int(pos_left * p_long)]

        pos_risk = (self.day_bp / pos_left) if self.long_same_day else min(self.overnight_bp, self.day_bp) / pos_left
        for e in pos_list:
            bar = self.get_day_data(e.ticker, self.date_current)
            if e.attrs.pos == strategy.Trade.LONG:
                volume = min(self.max_volume,
                             self.get_adjusted_volume(self.get_volume_comm_adjusted(pos_risk, bar.Open)))
                if volume > 0:
                    self.add_open_close_trade(
                        ticker=e.ticker,
                        entry_date=e.entry_date,
                        exit_date=e.entry_date if self.long_same_day else e.next_date,
                        direction=e.attrs.pos,
                        price=bar.Open,
                        stop=bar.Open,
                        volume=volume,
                        pos_risk=pos_risk)

            elif e.attrs.pos == strategy.Trade.SHORT:
                volume = min(self.max_volume,
                             self.get_adjusted_volume(self.get_volume_comm_adjusted(pos_risk, bar.Open)))
                if volume > 0:
                    self.add_open_close_trade(
                        ticker=e.ticker,
                        entry_date=e.entry_date,
                        exit_date=e.entry_date,
                        direction=e.attrs.pos,
                        price=bar.Open,
                        stop=bar.Open,
                        volume=volume,
                        pos_risk=pos_risk)

    # Add additional info
    def on_finish(self):
        self.add_line('Selected stocks range', '$%.2f..$%.2f' % (self.price_range[0], self.price_range[1]))
        self.add_line('Maximum volume per trade', '%d' % self.max_volume)
        self.add_line('Maximum risk for portfolio',
                      '%d%% (%d positions)' % (self.portfolio_risk * 100, self.portfolio_size))
        self.add_line('Small average volume skipped',
                      '%d, volume < %d' % (self.small_avg_volume_skipped, self.min_avg_volume))
        self.add_line('Long exits', 'same day' if self.long_same_day else 'next day')


if __name__ == '__main__':
    st_list = []
    # st1 = Earnings(name='Earnings x10', start_balance=5000, portfolio_size=10)
    # st1.run().plot_result()
    # st2 = Earnings(name='Earnings x15', start_balance=5000, portfolio_size=15)
    # st2.run().plot_result()

    '''
    st_list.append(Earnings(
        name='Earnings 2008-2010',
        date_start = datetime.datetime(2008, 1, 1),
        date_end = datetime.datetime(2010, 1, 1)).run())

    st_list.append(Earnings(
        name='Earnings 2010-2012',
        date_start=datetime.datetime(2010, 1, 1),
        date_end=datetime.datetime(2012, 1, 1)).run())

    st_list.append(Earnings(
        name='Earnings 2012-2014',
        date_start=datetime.datetime(2012, 1, 1),
        date_end=datetime.datetime(2014, 1, 1)).run())

    st_list.append(Earnings(
        name='Earnings 2014-2016',
        date_start=datetime.datetime(2014, 1, 1),
        date_end=datetime.datetime(2016, 1, 1)).run())

    st_list.append(Earnings(
        name='Earnings 2016-2018',
        date_start=datetime.datetime(2016, 1, 1),
        date_end=datetime.datetime(2018, 8, 1)).run())

    strategy.Strategy.compare_balances(st_list, drawdown=True)
    '''

    st_list.append(Earnings(name='Earnings, longs next day').run().plot_result(drawdown=True))

    # for year in range(2008, 2009):
    #     st = Earnings(
    #         name='Earnings %d' % year,
    #         start_balance=3000,
    #         portfolio_size=20,
    #         event_source=strategy.Strategy.EVENTS_PORTFOLIO_123,
    #         date_start=datetime.datetime(year, 1, 1),
    #         date_end=datetime.datetime(year, 12, 31))
    #     st.run()
    #     st_list.append(st)
    #

    # st3.run().plot_result()

    # strategy.Strategy.compare_balances([st1, st2, st3], drawdown=True)

    # ss = Earnings(
    #     disable_download=False,
    #     event_source=strategy.Strategy.EVENTS_PORTFOLIO_123,
    #     data_source=strategy.Strategy.DATA_ALPHA)
    #
    # ss.load_hyper_cache()
    # tickers, e = ss.get_events()
    # ss.download_all(tickers)
