import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pgportfolio.marketdata.globaldatamatrix

def plot_from_summary(dir_name):
    df = pd.read_csv('./train_package/' + dir_name + '/train_summary.csv')
    print(df['backtest_test_history'])

    raw_y = df['backtest_test_history'].values[0]
    raw_y = raw_y.split(',')
    raw_y = list(map(float, raw_y[:-1]))

    # y는 누적 수익률
    y = []
    for i in range(len(raw_y)):
        _y = raw_y[0]
        for j in range(1, i):
            _y *= raw_y[j]
        y.append(_y)



    plt.plot(list(range(len(y))),y)

    plt.show()