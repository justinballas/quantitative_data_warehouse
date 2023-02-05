import numpy as np
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
import requests
import os
import re

engine = create_engine('postgresql://postgres:password1@localhost:5432/financial_modelling_db')

api_key = os.environ.get('FMP_API_KEY')

symbols = pd.read_sql('select id, symbol from ticker_data.all_symbols', engine).set_index('symbol')

profiles = [requests.get(f'https://financialmodelingprep.com/api/v3/profile/{symbol}?apikey={api_key}').json() for symbol in list(symbols)]

end_date = dt.datetime.today().date()

# Return dataframe 
symbol_df = pd.read_sql('''
select symbol, id, coalesce(last_price_date, '1900-01-01') as last_price_date from ticker_data.all_symbols symb
	left join (
		select symbol_id, max(date) as last_price_date from ticker_data.fact_price_history fph
			group by symbol_id
	)max_dates
		on symb.id = max_dates.symbol_id
''', engine)

symbol_df.set_index('symbol', inplace=True)
symbol_list = list(symbol_df.index)

def get_price_history(symbol, end_date):
    '''
    Get price data for each stock
    '''
    # Get symbol ID
    symb_id = symbol_df.loc[symbol][0]
    # Determine starting date for data pull
    start_date = symbol_df.loc[symbol][1].date()
    # Define pattern for converting column names to snake case
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    
    try:
        # Script will not run if the last price date is equal to today's date
        # Else, get price history for delta between last price date and today's date
        if start_date >= end_date:
            pass
        else:
            price_hist = requests.get(f'https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={start_date.strftime("%Y-%m-%d")}&to={end_date.strftime("%Y-%m-%d")}&apikey={api_key}').json()
            if len(price_hist) > 0:
                price_hist_df = pd.DataFrame(price_hist['historical'])
                price_hist_df['date'] = pd.to_datetime(price_hist_df['date']).apply(lambda x: x.date())
                # Return empty dataframe if latest date returned is equal to the last price date from the DB
                if price_hist_df['date'].max() <= start_date:
                    pass
                else:    
                    new_colnames = [pattern.sub('_', col_name).lower() for col_name in price_hist_df.columns]

                    price_hist_df.rename(columns={list(price_hist_df.columns)[i]: new_colnames[i] for i in range(len(price_hist_df.columns))}, inplace=True) 

                    price_hist_df.insert(0, 'symbol_id', symb_id)
                    return price_hist_df
            else:
                pass
    except:
        print(f'{symbol} failed to load. May be delisted')
try:
    historical_prices = pd.concat([get_price_history(symbol, end_date) for symbol in symbol_list])
    historical_prices['date_actual'] = pd.to_datetime(historical_prices['date'])
    historical_prices['date_id'] = historical_prices['date'].apply(lambda x: int(x.strftime('%Y%m%d')))
    
    output_df_clean = historical_prices[['symbol_id', 'date_id', 'date_actual', 'open', 'high', 'low', 'close', 'adj_close', 'volume',
   'unadjusted_volume', 'change', 'change_percent', 'vwap', 'change_over_time']]

    try:
        output_df_clean.to_sql('fact_price_history', engine, schema='ticker_data', index=False, if_exists='replace')
    except: print('Unable to load to database')
except:
    print('Failed to concatenate dataframe')
print('Price History Uploaded Complete')