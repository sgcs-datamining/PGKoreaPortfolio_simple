import numpy as np
import pandas as pd
from pgportfolio.tools.data import panel_fillna
from pgportfolio.constants import *
from pgportfolio.tools.time_index import gen_time_index, unix_to_yyyymmdd
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
        time_index = pd.to_datetime(gen_time_index(unix_to_yyyymmdd(start), unix_to_yyyymmdd(end)))
        panel = pd.Panel(items=features, major_axis=assets, minor_axis=time_index, dtype=np.float32)

        connection = sqlite3.connect(DATABASE_DIR)
        try:
            for row_number, asset in enumerate(assets):
                for feature in features:
                    # NOTE: transform the start date to end date
                    if feature == "close":
                        sql = ("SELECT date AS date_norm, close "+
                               "FROM History "
                               "WHERE date_norm>={start} and date_norm<={end} "
                               "and date_norm%{period}=0 and asset=\"{asset}\"".format(
                               start=start, end=end, period=period, asset=asset))
                    elif feature == "open":
                        sql = ("SELECT date AS date_norm, open "+
                               "FROM History WHERE "
                               "date_norm>={start} and date_norm<={end} "
                               "and date_norm%{period}=0 and asset=\"{asset}\"".format(
                               start=start, end=end, period=period, asset=asset))
                    elif feature == "volume":
                        sql = ("SELECT date_norm, SUM(volume) "+
                               "FROM (SELECT date-(date%{period}) "
                               "AS date_norm, volume, asset FROM History) "
                               "WHERE date_norm>={start} and date_norm<={end} and asset=\"{asset}\" "
                               "GROUP BY date_norm".format(
                                period=period,start=start,end=end,asset=asset))
                    elif feature == "high":
                        sql = ("SELECT date_norm, MAX(high) "+
                               "FROM (SELECT date-(date%{period}) "
                               "AS date_norm, high, asset FROM History) "
                               "WHERE date_norm>={start} and date_norm<={end} and asset=\"{asset}\" "
                               "GROUP BY date_norm".format(
                                period=period,start=start,end=end,asset=asset))
                    elif feature == "low":
                        sql = ("SELECT date_norm, MIN(low) "+
                               "FROM (SELECT date-(date%{period}) "
                               "AS date_norm, low, asset FROM History) "
                               "WHERE date_norm>={start} and date_norm<={end} and asset=\"{asset}\" "
                               "GROUP BY date_norm".format(
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
