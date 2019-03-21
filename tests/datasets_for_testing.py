import pandas as pd
import numpy as np

SUB_DISTRICT = ['0000010001',
                '0000010001',
                '0000010002',
                '0000010002',
                '0000020001',
                '0000020002',
                '0000030001',
                '0000030001']
district = [sd[4:6] for sd in SUB_DISTRICT]
DATE = [2017, 2018, 2017, 2018, 2017, 2017, 2017, 2018]
VALUE_A = [110, 112, 120, 122, 210, 212, 310, 312]
VALUE_B = [1, 2, 3, 4, 5, 6, 7, 8]

# DATASET 1 - BASIC
df1 = pd.DataFrame({'sub_district': SUB_DISTRICT,
                    'date': DATE,
                    'district': district,
                    'value_A': VALUE_A,
                    'value_B': VALUE_B})

# DATASET 1 - AGGREGATED (SUM)
SUM_SUB_DISTRICT = [np.nan for i in range(7)]
SUM_DATE = [2017, 2018, 2017, 2017, 2018, 2017, 2018]
SUM_DISTRICT = ['01', '01', '02', '03', '03', '00', '00']
SUM_VALUE_A = [230, 234, 422, 310, 312, 962, 546]
SUM_VALUE_B = [4, 6, 11, 7, 8, 22, 14]

df1_agg_sum = pd.DataFrame({'sub_district': [*SUB_DISTRICT, *SUM_SUB_DISTRICT],
                            'date': [*DATE, *SUM_DATE],
                            'district': [*district, *SUM_DISTRICT],
                            'value_A': [*VALUE_A, *SUM_VALUE_A],
                            'value_B': [*VALUE_B, *SUM_VALUE_B]})

# DATASET 2
#df2 = df.loc[:7, ['sub_district', 'date']].copy()
#df2['value_A'] = [500, 500, 500, 500, 400, 400, 400, 400]
#df2['value_C'] = list('ABCDEFGH')


#df2['district'] = df2['sub_district'].str.slice(4, 6)

data_sets = {'df1': df1,
             'df1_agg_sum': df1_agg_sum}

if __name__ == '__main__':

    print('============== Available datasets ==============')
    for name in data_sets.keys():
        print('Data set: {k}'.format(k=name))
        print(data_sets[name])
        print('')




