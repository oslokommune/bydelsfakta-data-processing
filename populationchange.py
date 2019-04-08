#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd


def calculatePopulationChange(x):
    x['change'] = x.diff(periods=1)
    return x


def writeFile(x):
    geo = x.iloc[0]['geography']
    x.drop(columns=['geography']).to_json(
        geo + '.json', force_ascii=False, orient="records")


df = (pd.read_csv('befolkningen_etter_bydel_og_aldersgrupper.csv',
                  sep=";",  encoding='utf8')
      .drop(columns=['Obs'])
      .rename(columns={'antall': 'population', 'bydel2': 'geography', 'aargang': 'date'})
      .groupby(['geography', 'date'])
      .sum()
      .groupby(['geography'])
      .apply(lambda x: calculatePopulationChange(x))
      .fillna(False)
      .reset_index()
      .groupby(['geography'])
      .apply(lambda x: writeFile(x))
      )