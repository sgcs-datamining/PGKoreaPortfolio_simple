import datetime as dt

def unix_to_yyyymmdd(unixtime):
    """
    :param unixtime: unixtime
    :return: int 형식의 날짜 e.g. 20150101
    """
    ymd_dt = dt.datetime.fromtimestamp(unixtime)
    ymd_dt = ymd_dt.date().strftime("%Y%m%d")

    return int(ymd_dt)


def gen_time_index(fromdate, todate):
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

    mkt_closed_day = [dt.date(d // 10000, d % 10000 // 100, d % 100) for d in mkt_closed_day]

    time_list = []
    fromdate = dt.date(fromdate // 10000, fromdate % 10000 // 100, fromdate % 100)
    todate = dt.date(todate // 10000, todate % 10000 // 100, todate % 100)

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