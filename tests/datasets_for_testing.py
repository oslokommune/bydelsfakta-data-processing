import pandas as pd
import numpy as np

# DATASET 1 - ORIGINAL
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
VALUE_C = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]
VALUE_D = [400, 400, 400, 400, 600, 600, 600, 600]

df1 = pd.DataFrame({'delbydelid': SUB_DISTRICT,
                    'date': DATE,
                    'district': district,
                    'value_A': VALUE_A,
                    'value_B': VALUE_B,
                    'value_C': VALUE_C,
                    'value_D': VALUE_D})

# DATASET 1 - aggregated (sum)
SUM_SUB_DISTRICT = [np.nan for i in range(7)]
SUM_DATE = [2017, 2018, 2017, 2017, 2018, 2017, 2018]
SUM_DISTRICT = ['01', '01', '02', '03', '03', '00', '00']
SUM_VALUE_A = [230, 234, 422, 310, 312, 962, 546]
SUM_VALUE_B = [4, 6, 11, 7, 8, 22, 14]

df1_agg_sum = pd.DataFrame({'delbydelid': [*SUB_DISTRICT, *SUM_SUB_DISTRICT],
                            'date': [*DATE, *SUM_DATE],
                            'district': [*district, *SUM_DISTRICT],
                            'value_A': [*VALUE_A, *SUM_VALUE_A],
                            'value_B': [*VALUE_B, *SUM_VALUE_B]})


# DATASET 2 - This dataset is a bit smaller and more intuitive than the first.
# df2_org - Original data, four sub_districts in a total of two districts
df2_org = pd.DataFrame({'date': [2017, 2017, 2018, 2018],
                        'delbydelid': ['0300010001', '0300020001', '0300010001', '0300020001'],
                        'district': ['01', '02', '01', '02'],
                        'mean_income': [400000, 500000, 420000, 525000],
                        'inhabitants': [2000, 3000, 2200, 3300]})

# df2_agg_districts - New rows after aggregating sub_districts to districts - almost the same in this simple example.
df2_agg_districts = df2_org.copy()
df2_agg_districts['delbydelid'] = [np.nan, np.nan, np.nan, np.nan]

# df2_agg_Oslo - New rows after aggregating districts to Oslo total.
df2_agg_Oslo = pd.DataFrame({'date': [2017, 2018],
                             'delbydelid': [np.nan, np.nan],
                             'district': ['00', '00'],
                             'mean_income': [460000.0, 483000.0],
                             'inhabitants': [5000, 5500]})

# df2_agg_total - All the rows in union
df2_agg_total = pd.concat((df2_org, df2_agg_districts, df2_agg_Oslo), axis=0, sort=True)

# DATA SET COLLECTION
data_sets = {'df1': df1,
             'df1_agg_sum': df1_agg_sum,
             'df2_org': df2_org,
             'df2_agg_districts': df2_agg_districts,
             'df2_agg_Oslo': df2_agg_Oslo,
             'df2_agg_total': df2_agg_total}

if __name__ == '__main__':

    print('============== Available datasets ==============')
    for name in data_sets.keys():
        print('Data set: {k}'.format(k=name))
        print(data_sets[name])
        print('')
