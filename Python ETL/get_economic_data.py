import numpy as np
import pandas as pd
import datetime as dt
from fredapi import Fred
from sqlalchemy import create_engine
import os
import psycopg2

engine = create_engine('postgresql://postgres:Qsdrgyji9!@localhost:5432/financial_modelling_db')

api_key = os.environ.get('FRED_API_KEY')

fred = Fred(api_key=api_key)

leading_indicators = {'man_hours':'AWHAEMAN', 'initail_claims':'ICSA', 'new_manufacturing_orders':'DGORDER',
           'new_orders_capgoods':'ANDENO', 'residential_permits':'PERMIT',
          'money_supply':'WM2NS', 'int_rate_spread':'T10Y2Y', 'cons_sentiment':'UMCSENT'}

start = '2010-01-01'
end = dt.date.today()

metrics = []
for metric in leading_indicators.keys():
    df = pd.DataFrame(fred.get_series(leading_indicators[metric], observation_start=start, observation_end=end))
    df.index.rename('date', inplace=True)
    df.rename(columns={0:'value'}, inplace=True)
    df.insert(loc=0, column='metric', value=metric)
    df = df.reset_index().set_index(['metric','date'])
    metrics.append(df)

metrics = pd.concat(metrics)

metrics.reset_index()

# metrics.to_sql('economic_data', engine, schema='economic_data', index=False, if_exists='replace')