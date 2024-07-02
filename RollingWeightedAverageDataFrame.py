from pandas.core.window.rolling import RollingAndExpandingMixin


class RollingWeightedAverageDataFrame:

    def __init__(self, df):
        self.df = df
        self.col_names = list(df.columns)
        assert len(self.col_names) == 2,"Unexpected input, dataframe should have 2 columns"

    def rolling(self, window, min_periods=1):
        self.window = window
        self.min_periods = min_periods
        return self

    def weighted_average(self):
        self.df['mul'] = self.df[self.col_names[0]] * self.df[self.col_names[1]]

        def _weighted_average(x):
            return (x['mul'].sum() / x[self.col_names[1]].sum())

        RollingAndExpandingMixin.weighted_average = _weighted_average

        return self.df[[self.col_names[0], self.col_names[1], 'mul']].rolling(window=self.window, min_periods=self.min_periods).weighted_average()