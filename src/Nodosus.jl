module Nodosus

import JuliaDB

function remove_outliers(data, Tukey_fences, Tukey_freq)
end


# def remove_outliers(dataframe, Tukey_fences, Tukey_freq):
#     ss = ()
#     for n, s in dataframe.items():
#         k, f = remove_outliers_lambdas[n]
#         # rename from longnames to tags, and rescale
#         s = f(s.copy(deep=True).rename(k))
#         if k not in ('ODT_OS', 'ODT_co'):
#             # remove repeted values
#             x = s.ffill().values
#             m = x[1 :] == x[: -1]
#             s.iloc[1 :].mask(m, inplace=True)
#             # remove extreme values, grouping to acomodate drift
#             for _, t in s.groupby(pandas.Grouper(freq=Tukey_freq)):
#                 x = t.values
#                 if not numpy.isnan(x).all():
#                     # adapted from https://en.wikipedia.org/wiki/Outlier#Tukey's_fences
#                     q1, q2, q3 = numpy.quantile(t.dropna(), [0.25, 0.50, 0.75])
#                     t.mask(x < q2 + Tukey_fences * (q1 - q2), inplace=True)
#                     t.mask(x > q2 + Tukey_fences * (q3 - q2), inplace=True)
#         ss += s,
#     return pandas.concat(ss, axis=1, copy=False)
#
#
# process_control_lambdas = {
#     'ODT_OS':
#     lambda df: 0.0,
#     'ODT_co':
#     lambda df: df['ODT'].values,
# }
#
#
# def process_control(dataframe):
#     for k, f in process_control_lambdas.items():
#         if k in dataframe:
#             try:
#                 dataframe[k].fillna(f(dataframe), inplace=True)
#             except:
#                 pass
#
#
# def compute_sensors(dataframe, freqH):
#     compute_sensors_lambdas = {
#         'Day':
#         lambda df: (
#             (
#                 (
#                     df.index.dayofweek * 24 +
#                     df.index.hour
#                 ) * 60 +
#                 df.index.minute
#             ) * 60 +
#             df.index.second
#         ) / (24 * 60 * 60),
#         'ODT_OS':
#         lambda df: df['ODT_co'].values - df['ODT'].values,
#         'ODT_co':
#         lambda df: df['ODT_OS'].values + df['ODT'].values,
#         'Flow':
#         lambda df: df['Volume'].diff().values * freqH,
#         'Power':
#         lambda df: df['Energy'].diff().values * freqH,
#         'PSTD':
#         lambda df: df['PS-Supply_T'] - df['PS-Return_T'],
#         'SSTD':
#         lambda df: df['SS-Supply_T'] - df['SS-Return_T'],
#         'GTD':
#         lambda df: df['PS-Supply_T'] - df['SS-Supply_T'], # District Heating and Cooling, figure 9.21 (p. 393) [°C]
#         'LTD':
#         lambda df: df['PS-Return_T'] - df['SS-Return_T'], # District Heating and Cooling, figure 9.21 (p. 393) [°C]
#         'LMTD':
#         lambda df: (df['GTD'] - df['LTD']) / (numpy.log(df['GTD']) - numpy.log(df['LTD'])), # District Heating and Cooling, equation 9.7 (p. 394) [°C]
#         'AK':
#         lambda df: (df['Power'] / df['LMTD']), # District Heating and Cooling, equation 9.6 (p. 394) [kW / °C]
#         'NTU':
#         lambda df: df['PSTD'] / df['LMTD'], # District Heating and Cooling, equation 9.10 (p. 395) []
#     }
#     for k, f in compute_sensors_lambdas.items():
#         if k not in dataframe:
#             try:
#                 dataframe[k] = f(dataframe)
#             except:
#                 pass
#     return dataframe

function process_data(data; Tukey_fences=3, Tukey_freq='W', freqH=1, periodsH=1)
end

# def process_dataframe(dataframe, Tukey_fences=3, Tukey_freq='W', freqH=1, periodsH=1):
#     # shortcircuit on empty
#     if dataframe.empty:
#         return pandas.DataFrame()
#     # remove repeted values and apply Tukey's fences
#     df = remove_outliers(dataframe, Tukey_fences, Tukey_freq)
#     # resample
#     freq = '{!s}T'.format(60 // freqH)
#     df = df.resample(freq).mean()
#     # process missing control values, interpreting missing values as no control
#     process_control(df)
#     # interpolate
#     df.interpolate(limit=(freqH * periodsH), limit_area='inside', inplace=True)
#     # compute missing sensors
#     compute_sensors(df, freqH)
#     return df

end
