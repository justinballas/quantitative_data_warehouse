import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
import psycopg2
import requests
import os
import re

engine = create_engine('postgresql://postgres:password1@localhost:5432/financial_modelling_db')

# # Get symbols from database
symbols = pd.read_sql('select id, symbol from ticker_data.all_symbols', engine).set_index('symbol')

api_key = os.environ.get('FMP_API_KEY')

profiles = [requests.get(f'https://financialmodelingprep.com/api/v3/profile/{symbol}?apikey={api_key}').json() for symbol in list(symbols.index)]

# Create dataframe of stock profiles
# Iterate through API request, if request returns nothing, repalce with emtpy dict
profiles_df = pd.DataFrame([profiles[i][0] if len(profiles[i])>0 else dict() for i in range(len(profiles))])

# Remove empty profiles
profiles_df = profiles_df[profiles_df['symbol'].notnull()]

def camel_to_snake_case(df):
    '''
    Convert dataframe column names from snake case to camel case
    '''
    pattern = re.compile(r'(?<!^)(?=[A-Z])')

    new_colnames = [pattern.sub('_', col_name).lower() for col_name in df.columns]
    
    df = df.rename(columns={list(df.columns)[i]: new_colnames[i] for i in range(len(df.columns))})
    return df

profiles_df = camel_to_snake_case(profiles_df)

profiles_df_id = symbols.merge(profiles_df, 'left', left_index=True, right_on='symbol')

engine.execute('delete from ticker_data.dim_stock_profile')

profiles_df_id.to_sql('dim_stock_profile', engine, schema='ticker_data', index=False, if_exists='append')

