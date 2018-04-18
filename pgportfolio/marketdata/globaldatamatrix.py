import numpy as np
import pandas as pd
from pgportfolio.tools.data import panel_fillna
from pgportfolio.constants import *
import sqlite3
from datetime import datetime
import datetime as dt
import logging


class HistoryManager:
    # if offline ,the asset_list could be None
    # NOTE: return of the sqlite results is a list of tuples, each tuple is a row
    def __init__(self, asset_number, end, volume_average_days=1, volume_forward=0):
        self.initialize_db()
        self.__storage_period = FIVE_MINUTES  # keep this as 300
        self._asset_number = asset_number
        self.__volume_forward = volume_forward
        self.__volume_average_days = volume_average_days
        self.__assets = None

    @property
    def assets(self):
        return self.__assets

    def initialize_db(self):
        with sqlite3.connect(DATABASE_DIR) as connection:
            cursor = connection.cursor()
            cursor.execute('CREATE TABLE IF NOT EXISTS History (date INTEGER,'
                           ' asset varchar(20), high FLOAT, low FLOAT,'
                           ' open FLOAT, close FLOAT, volume FLOAT, '
                           ' quoteVolume FLOAT, weightedAverage FLOAT,'
                           'PRIMARY KEY (date, asset));')
            connection.commit()

    @staticmethod
    def _unix_to_yyyymmdd(unixtime):
        """
        :param unixtime: unixtime
        :return: int 형식의 날짜 e.g. 20150101
        """
        ymd_dt = dt.datetime.fromtimestamp(unixtime)
        ymd_dt = ymd_dt.date().strftime("%Y%m%d")

        return int(ymd_dt)

    @staticmethod
    def _gen_time_index(fromdate, todate):

        """
        fromdate : int 20150101 형식
        todate : int 20180101 형식
        증시 개장 시간 중 가격 데이터가 존재하는 시간들을 datetime type들의 list 으로 반환
        2016-07-29까지는 09:05, 09:10, ... , 14:45, 14:50, 15:00
        2016-08-01부터는 09:05, 09:10, ... , 15:15, 15:20, 15:30

        """

        # 설날, 추석 등의 휴장일 리스트. 현재 2015-01-01 ~ 2018-03-01까지의 기간만 고려하였음
        mkt_closed_day = [20150101, 20150218, 20150219, 20150220, 20150501, 20150505, 20150525, 20150814,
                          20150928, 20150929, 20151009, 20151225, 20151231, 20160101, 20160208, 20160209,
                          20160210, 20160301, 20160413, 20160501, 20160505, 20160606, 20160815, 20160914,
                          20160915, 20160916, 20161003, 20161009, 20161225, 20161230, 20170127, 20170130,
                          20170301, 20170501, 20170503, 20170505, 20170509, 20170606, 20170815, 20171003,
                          20171004, 20171005, 20171006, 20171009, 20171225, 20171229, 20180101, 20180215,
                          20180216, 20180301]

        mkt_closed_day = [dt.date(d//10000, d%10000//100, d%100) for d in mkt_closed_day]

        time_list = []
        fromdate = dt.date(fromdate//10000, fromdate%10000//100, fromdate%100)
        todate = dt.date(todate//10000, todate%10000//100, todate%100)

        date = fromdate
        while date < todate:

            # 주말
            if date.weekday() in (5, 6):
                date = date + dt.timedelta(days=1)
                continue

            # 공휴일
            if date in mkt_closed_day:
                date = date + dt.timedelta(days=1)
                continue

            t = dt.datetime(date.year, date.month, date.day, 9, 5)
            time_list.append(t)

            if t < dt.datetime(2016, 8, 1):
                while t < dt.datetime(date.year, date.month, date.day, 14, 50):
                    t = t + dt.timedelta(minutes=5)
                    time_list.append(t)
            else:
                while t < dt.datetime(date.year, date.month, date.day, 15, 20):
                    t = t + dt.timedelta(minutes=5)
                    time_list.append(t)

            time_list.append(t + dt.timedelta(minutes=10))

            date = date + dt.timedelta(days=1)

        return time_list

    def get_global_data_matrix(self, start, end, period=300, features=('close',)):
        """
        :return a numpy ndarray whose axis is [feature, asset, time]
        """
        return self.get_global_panel(start, end, period, features).values

    def get_global_panel(self, start, end, period=300, features=('close',)):
        """
        :param start/end: linux timestamp in seconds
        :param period: time interval of each data access point
        :param features: tuple or list of the feature names
        :return a panel, [feature, asset, time]
        """
        start = int(start - (start%period))
        end = int(end - (end%period))
        assets = self.select_assets(start=end - self.__volume_forward - self.__volume_average_days * DAY,
                                  end=end-self.__volume_forward)
        self.__assets = assets


        if len(assets)!=self._asset_number:
            raise ValueError("the length of selected assets %d is not equal to expected %d"
                             % (len(assets), self._asset_number))

        logging.info("feature type list is %s" % str(features))
        self.__checkperiod(period)

        # time_index를 한국 장 시간에 딱 맞추기 위해 아래 메소드 사용
        time_index = pd.to_datetime(self._gen_time_index(self._unix_to_yyyymmdd(start), self._unix_to_yyyymmdd(end)))
        panel = pd.Panel(items=features, major_axis=assets, minor_axis=time_index, dtype=np.float32)

        connection = sqlite3.connect(DATABASE_DIR)
        try:
            for row_number, asset in enumerate(assets):
                for feature in features:
                    # NOTE: transform the start date to end date
                    if feature == "close":
                        sql = ("SELECT date AS date_norm, close FROM History WHERE"
                               " date_norm>={start} and date_norm<={end}" 
                               " and date_norm%{period}=0 and asset=\"{asset}\"".format(
                               start=start, end=end, period=period, asset=asset))
                    elif feature == "open":
                        sql = ("SELECT date AS date_norm, open FROM History WHERE"
                               " date_norm>={start} and date_norm<={end}" 
                               " and date_norm%{period}=0 and asset=\"{asset}\"".format(
                               start=start, end=end, period=period, asset=asset))
                    elif feature == "volume":
                        sql = ("SELECT date_norm, SUM(volume)"+
                               " FROM (SELECT date-(date%{period}) "
                               "AS date_norm, volume, asset FROM History)"
                               " WHERE date_norm>={start} and date_norm<={end} and asset=\"{asset}\""
                               " GROUP BY date_norm".format(
                                    period=period,start=start,end=end,asset=asset))
                    elif feature == "high":
                        sql = ("SELECT date_norm, MAX(high)" +
                               " FROM (SELECT date-(date%{period})"
                               " AS date_norm, high, asset FROM History)"
                               " WHERE date_norm>={start} and date_norm<={end} and asset=\"{asset}\""
                               " GROUP BY date_norm".format(
                                    period=period,start=start,end=end,asset=asset))
                    elif feature == "low":
                        sql = ("SELECT date_norm, MIN(low)" +
                                " FROM (SELECT date-(date%{period})"
                                " AS date_norm, low, asset FROM History)"
                                " WHERE date_norm>={start} and date_norm<={end} and asset=\"{asset}\""
                                " GROUP BY date_norm".format(
                                    period=period,start=start,end=end,asset=asset))
                    else:
                        msg = ("The feature %s is not supported" % feature)
                        logging.error(msg)
                        raise ValueError(msg)
                    serial_data = pd.read_sql_query(sql, con=connection,
                                                    parse_dates=["date_norm"],
                                                    index_col="date_norm")
                    # 한국 시간(GMT+9)에 맞추기
                    serial_data.set_index(serial_data.index + dt.timedelta(hours=9), inplace=True)
                    panel.loc[feature, asset, serial_data.index] = serial_data.squeeze()
                    panel = panel_fillna(panel, "both")
        finally:
            connection.commit()
            connection.close()
        return panel

    # select top asset_number of assets by volume from start to end
    def select_assets(self, start, end):

        logging.info("select assets offline from %s to %s" % (datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M'),
                                                                datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M')))
        connection = sqlite3.connect(DATABASE_DIR)
        try:
            cursor=connection.cursor()
            cursor.execute('SELECT asset,SUM(volume) AS total_volume FROM History WHERE'
                           ' date>=? and date<=? GROUP BY asset'
                           ' ORDER BY total_volume DESC LIMIT ?;',
                           (int(start), int(end), self._asset_number))
            assets_tuples = cursor.fetchall()

            if len(assets_tuples)!=self._asset_number:
                logging.error("the sqlite error happend")
        finally:
            connection.commit()
            connection.close()
        assets = []
        for tuple in assets_tuples:
            assets.append(tuple[0])

        logging.debug("Selected assets are: "+str(assets))
        return assets

    def __checkperiod(self, period):
        if period == FIVE_MINUTES:
            return
        elif period == FIFTEEN_MINUTES:
            return
        elif period == HALF_HOUR:
            return
        elif period == TWO_HOUR:
            return
        elif period == FOUR_HOUR:
            return
        elif period == DAY:
            return
        else:
            raise ValueError('peroid has to be 5min, 15min, 30min, 2hr, 4hr, or a day')
