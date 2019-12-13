module Sputnik

"""
## HEKA
* Primary differential temperature
* Volume weighted primary differential temperature
* Primary return temperature
* Volume weighted primary return temperature
* Volume per energy
* Overflow
* Balance temperature
* Domestic hot water quota
* Energy signature
"""

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

end
