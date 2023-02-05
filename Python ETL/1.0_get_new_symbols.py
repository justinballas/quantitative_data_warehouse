import pandas as pd
import datetime as dt
from sqlalchemy import MetaData, Table,  create_engine
import requests
import os

engine = create_engine('postgresql://postgres:password1@localhost:5432/financial_modelling_db')
connection = engine.connect()

api_key = os.environ.get('FMP_API_KEY')

# Get list of symbols that are alrady in database
loaded_symbols = {i[0]: i[1] for i in connection.execute('select * from ticker_data.all_symbols').fetchall()}

# Get list of available symbols from FMP API
request = requests.get(f'https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={api_key}').json()
avail_symbols_list = pd.DataFrame({'symbols':[request[i]['symbol'] for i in range(len(request))]})

# Determine new symbols that are not already in database
new_symbols_list = avail_symbols_list[~avail_symbols_list['symbols'].isin(loaded_symbols)]['symbols']

if len(loaded_symbols) == 0:
    start_id = 1
else:
    start_id = list(loaded_symbols)[-1] + 1
    
new_symbol_ids = [i for i in range(start_id, len(new_symbols_list))]

values = [{'id': new_symbol_ids[i], 'symbol':new_symbols_list[i]} for i in range(len(new_symbol_ids))]
new_symbols_df = pd.DataFrame(values)
new_symbols_df['load_date'] = dt.datetime.today()

# Push new stock profiles to sql
new_symbols_df.to_sql('all_symbols', engine, schema='ticker_data', index=False, if_exists='append')