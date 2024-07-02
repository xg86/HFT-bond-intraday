import pandas as pd
import numpy as np
from RollingWeightedAverageDataFrame import RollingWeightedAverageDataFrame

freq = '5Min'
window = 5
#worksheets = ['2022-12-19', '2022-12-20', '2022-12-21', '2022-12-22', '2022-12-23', '2022-12-27']
worksheets = ['2022-12-22', '2022-12-23', '2022-12-27']
filenames = ['220215']
'''
wm = lambda x: df.loc[x.index, "volume"].sum()
def agg_func(df):
    return df.groupby(pd.Grouper(freq='1Min')).agg(last_weighted=("last", wm), volume_sum=("volume", "sum"))
'''
def wavg(group, avg_name, weight_name):
    """ http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
    In rare instance, we may not have weights, so just return the mean. Customize this if your business case
    should return otherwise.
    """
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return d.mean()

def wavg_rolling(d1):
    """ http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
    In rare instance, we may not have weights, so just return the mean. Customize this if your business case
    should return otherwise.
    """
    print("d1:" + d1)
    return d1.max()
    #print("d2:" + d2)



def get_twap(df, col_name):
    df['ts_date'] = pd.to_datetime(df['create_time'])
    df['ts_lag'] = df['ts_date'].diff().apply(lambda x: x/np.timedelta64(1, 's')).fillna(0).astype('int64')
    #df.to_csv(filename + '_' + sheet + '_' + freq + "_t_lag.csv")
    df.set_index('ts_date', inplace=True)
    # df.drop('Time', axis=1, inplace=True)
    # df1 = df.groupby('pre_close').apply(agg_func)
    # df1 = df.groupby(pd.Grouper(freq='1Min')).apply(lambda x: x * df.loc[x.index, "volume"].sum() /df.volume.sum())
    df['twap_roll_5'] = RollingWeightedAverageDataFrame(df[['price', 'ts_lag']]).rolling(window=freq).weighted_average()
    df['twap_roll_10'] = RollingWeightedAverageDataFrame(df[['price', 'ts_lag']]).rolling(window='10Min').weighted_average()
    #df['twap_rolling'] = df.rolling(window = '5min').apply(wavg_rolling)

    #df2 = pd.DataFrame({'ts_date': ps1.index, col_name:  ps1.values})
    #return df2.fillna(method='ffill')

def zscore(x, window):
    r = x.rolling(window=window)
    m = r.mean().shift(1)
    s = r.std(ddof=0).shift(1)
    z = (x-m)/s
    return z, m

#request_file= 'C://git//TreasuryFutureTrading//wind//0830_last.csv'
#df = pd.read_csv(request_file, encoding='utf-8')
path_file= 'F://meridian//债券//'
path_proj= 'D://git//HFT-bond-intraday//'
file_ext = '-trade.xlsx'

#filenames = ['国债期货1107.xlsx','国债期货1108.xlsx','国债期货1109.xlsx','国债期货1110.xlsx','国债期货1111.xlsx','国债期货1114.xlsx','国债期货1115.xlsx']

lunch_brk_end =  pd.to_datetime('13:00:00', format='%H:%M:%S').time()
lunch_brk_start =  pd.to_datetime('12:00:00', format='%H:%M:%S').time()
for sheet in worksheets:
    print("trade date:" + sheet)
    #twap_dfs = []
    for filename in filenames:
        print("filename:" + filename)
        data_df = pd.read_excel(path_file+filename+file_ext, sheet_name=sheet)
        #tf_data_df = pd.read_excel(filename, sheet_name=instruments[1])
        #t_data_df = pd.read_excel(filename, sheet_name=instruments[2])
        get_twap(data_df, filename)
        data_df['time'] = data_df['create_time'].dt.time
        dfOutput = data_df[(data_df['time'] > lunch_brk_end)
                            | (data_df['time'] <= lunch_brk_start)]
        dfOutput['diff'] = dfOutput['price'] - dfOutput['twap_roll_5']
        dfOutput['diff2'] = dfOutput['twap_roll_5'] - dfOutput['twap_roll_10']
        dfOutput['zcore-' + str(window)], dfOutput['mean'] = zscore(dfOutput['diff'], window)

        dfOutput['zcore-' + str(window)] = dfOutput['zcore-' + str(window)].fillna(method='ffill')

        dfOutput['2zcore-' + str(window)], dfOutput['mean'] = zscore(dfOutput['diff2'], window)
        dfOutput.replace([np.inf, -np.inf], np.nan, inplace=True)
        dfOutput['2zcore-' + str(window)] = dfOutput['2zcore-' + str(window)].fillna(method='ffill')

        dfOutput.to_csv(path_proj + sheet + '_' + filename + '_' + freq + "_twap_zscore_rolling.csv")


    '''dfOutput = pd.concat([twap_dfs[0]['ts_date'],
                          twap_dfs[0][filenames[0]],
                          twap_dfs[1]['ts_date'],
                          twap_dfs[1][filenames[1]],
                          ],
                          axis=1)
'''
    #dfOutput = data_df.merge(twap_df, on='create_time', how='outer')
    #dfOutput = pd.merge_asof(data_df, twap_df, left_on='create_time', right_on='ts_date', tolerance=pd.Timedelta(freq))

    #dfOutput = tmp.merge(twap_dfs[2], on='ts_date', how='outer')
    #dfOutput = pd.concat(twap_dfs)
    #dfOutput = dfOutput.dropna()

    #dfOutput['time'] = dfOutput['create_time'].dt.time
    #dfOutput = dfOutput[['ts_date', 'time', '220210', '220215']]
    #dfOutput['diff-220115'] = dfOutput['220210'].diff()


    # dfOutput['diff2'] = dfOutput['220210'] - dfOutput['220215']
    # dfOutput['zcore2-' + str(window)], dfOutput['mean2'] = zscore(dfOutput['diff2'], window)




'''
wm = lambda x: (x * df.loc[x.index, "flow"]).sum() / df.flow.sum()

def agg_func(df):
    return df.groupby(pd.Grouper(freq='5Min')).agg(latency_sum=("latency", "sum"), duration_weighted=("duration", wm))


request_file= 'C://git//TreasuryFutureTrading//wind//1.csv'
df = pd.read_csv(request_file, encoding='utf-8')

#convert to datetimes
df['ts_date'] = pd.to_datetime(df['ts_ms'])
df.set_index('ts_date', inplace=True)

df1 = df.groupby(["a", "b", "c"]).apply(agg_func)
print(df1)
'''