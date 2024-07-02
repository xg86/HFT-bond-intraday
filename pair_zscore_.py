import pandas as pd
import numpy as np
from RollingWeightedAverageDataFrame import RollingWeightedAverageDataFrame

freq = '5Min'
window = 10
window2 = 15
#worksheets = ['2022-12-19', '2022-12-20', '2022-12-21', '2022-12-22', '2022-12-23', '2022-12-27']
worksheets = ['2023-01-12']
filenames = ['220220']


def get_twap(df, col_name):
    df['ts_date'] = pd.to_datetime(df['create_time'])
    df['ts_lag'] = df['ts_date'].diff().apply(lambda x: x/np.timedelta64(1, 's')).fillna(0).astype('int64')
    df.set_index('ts_date', inplace=True)
    df['twap_roll_'+freq] = RollingWeightedAverageDataFrame(df[['price', 'ts_lag']]).rolling(window=freq).weighted_average()
    df['twap_roll_10'] = RollingWeightedAverageDataFrame(df[['price', 'ts_lag']]).rolling(window='10Min').weighted_average()

def zscore(x, window):
    r = x.rolling(window=window)
    m = r.mean().shift(1)
    s = r.std(ddof=0).shift(1)
    z = (x-m)/s
    return z, m

def zscore2(x, window1, window2):
    mv_ave1 = x.rolling(window=window1, center=False).mean().shift(1)
    mv_ave2 = x.rolling(window=window2, center=False).mean().shift(1)
    # mv_std = ratios.rolling(window=window2, center=False).std() #wrong, it should not rolling window STD
    mv_std = x.rolling(window=window2, center=False).std(ddof=0).shift(1)
    zscore = (mv_ave1 - mv_ave2) / mv_std
    return  zscore


path_file= 'F://meridian//债券//'
path_proj= 'D://git//HFT-bond-intraday//'
file_ext = '-trade.xlsx'

#filenames = ['国债期货1107.xlsx','国债期货1108.xlsx','国债期货1109.xlsx','国债期货1110.xlsx','国债期货1111.xlsx','国债期货1114.xlsx','国债期货1115.xlsx']

lunch_brk_end =  pd.to_datetime('13:00:00', format='%H:%M:%S').time()
lunch_brk_start =  pd.to_datetime('12:00:00', format='%H:%M:%S').time()
for sheet in worksheets:
    print("trade date:" + sheet)
    for filename in filenames:
        print("filename:" + filename)
        data_df = pd.read_excel(path_file+filename+file_ext, sheet_name=sheet)
        get_twap(data_df, filename)
        data_df['time'] = data_df['create_time'].dt.time
        dfOutput = data_df[(data_df['time'] > lunch_brk_end)
                            | (data_df['time'] <= lunch_brk_start)]
        dfOutput['diff'] = dfOutput['price'] - dfOutput['twap_roll_'+freq]
        #dfOutput['diff2'] = dfOutput['twap_roll_5'] - dfOutput['twap_roll_10']
        dfOutput['zcore-' + str(window)], dfOutput['mean'] = zscore(dfOutput['diff'], window)
        dfOutput.replace([np.inf, -np.inf], np.nan, inplace=True)
        dfOutput['zcore-' + str(window)] = dfOutput['zcore-' + str(window)].fillna(method='ffill')

        dfOutput['zcore2-' + str(window) + '-' + str(window2) ] = zscore2(dfOutput['diff'], window, window2)
        dfOutput.replace([np.inf, -np.inf], np.nan, inplace=True)
        dfOutput['zcore2-' + str(window) + '-' + str(window2)] = dfOutput['zcore2-' + str(window) + '-' + str(window2)].fillna(method='ffill')

        #dfOutput['2zcore-' + str(window)], dfOutput['mean'] = zscore(dfOutput['diff2'], window)
        #dfOutput.replace([np.inf, -np.inf], np.nan, inplace=True)
        #fOutput['2zcore-' + str(window)] = dfOutput['2zcore-' + str(window)].fillna(method='ffill')

        dfOutput.to_csv(path_proj + sheet + '_' + filename + '_' + freq + "_twap_zscore_rolling.csv")


