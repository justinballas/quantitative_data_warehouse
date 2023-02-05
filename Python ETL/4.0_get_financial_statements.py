import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
import psycopg2
import requests
import os
import re

engine = create_engine('postgresql://postgres:password1@localhost:5432/financial_modelling_db')
print('Database Cnnection Made')

# # Get symbols from database
symbols = pd.read_sql('select id, symbol from ticker_data.all_symbols', engine).set_index('symbol')

api_key = os.environ.get('FMP_API_KEY')

def fiscal_quarters_between(date):
        today = dt.datetime.now().date()
        delta = today - date
        return delta.days // 90

def get_statement_delta(symbol_id, symbol, last_date, last_year, last_quarter, statement_type):
        if last_date == None:
                limit = 400
        elif last_quarter != current_quarter or last_year != current_year:
                limit = fiscal_quarters_between(last_date)
                if limit == 0:
                        pass
        elif last_quarter == current_quarter and last_year == current_year: 
                pass
            
        if limit == 0:
                limit = 1
        else:
                limit = limit
                
        response = requests.get(f'https://financialmodelingprep.com/api/v3/{statement_type}/{symbol}?period=quarter&limit={limit}&apikey={api_key}').json()

        df = pd.json_normalize(response)

        pattern = re.compile(r'(?<!^)(?=[A-Z])')

        new_colnames = [pattern.sub('_', col_name).lower() for col_name in df.columns]

        df.rename(columns={list(df.columns)[i]: new_colnames[i] for i in range(len(df.columns))}, inplace=True)

        # print(symbol, symbol_id, last_date, limit, len(df))

        df['date'] = pd.to_datetime(df['date'])

        df = df.drop(['period', 'calendar_year'], axis=1).rename(columns={'date':'date_actual'})
        df['date_id'] = df['date_actual'].apply(lambda x: x.strftime('%Y%m%d')).astype(int)
        df['symbol_id'] = symbol_id

        if df['date_actual'].max().date() != last_date:

                return df
        else:
                return df.drop(df.index)

# retrieve current date for comparison
current_date = pd.read_sql('''
    WITH current_date1 AS (
    SELECT current_date AS date
    )
    SELECT dd.year_actual, dd.quarter_actual
    FROM current_date1 c
    left JOIN dim_date dd
        ON c.date = dd.date_actual;
''', engine)

current_year = current_date.loc[0, 'year_actual']
current_quarter = current_date.loc[0, 'quarter_actual']

print('Beginning Statements Download...')

# ----------------------------- income statement ---------------------------------
# get date info for comparison
print('Bginning Income Statement Download...')

income_statement_info = pd.read_sql('''
    WITH max_dates AS (
        select as2.id as symbol_id
            , as2.symbol
            , max(fis.date_actual) as date_actual
            , max(fis.date_id) as date_id
            from ticker_data.all_symbols as2
                left join ticker_data.fact_income_statement fis
                    on as2.id = fis.symbol_id 
            group by as2.id
    )
    SELECT m.symbol_id, m.symbol, m.date_actual, dd.year_actual, dd.quarter_actual  
    FROM max_dates m
    left join dim_date dd
        on m.date_id = dd.date_dim_id
            order by 1;
''', engine).set_index('symbol_id')

income_statement_df_list = []
for row in income_statement_info.iterrows():
        symbol_id = row[0]
        symbol = row[1][0]
        last_date = row[1][1]
        last_year = row[1][2]
        last_quarter = row[1][3]

        temp_df = get_statement_delta(symbol_id=symbol_id, symbol=symbol, last_date=last_date, last_year=last_year
                , last_quarter=last_quarter, statement_type='income-statement')
        income_statement_df_list.append(temp_df)

income_statements_df = pd.concat(income_statement_df_list).sort_values('link', ascending=False).drop_duplicates(['date_actual', 'symbol'])

income_statements_df.to_sql('fact_income_statement', engine, schema='ticker_data', if_exists='append', index=False)

print('Income Statement Download Complete')

# ----------------------------- balance sheet ---------------------------------

print('Bginning Balance Sheet Download...')

balance_sheet_info = pd.read_sql('''
    WITH max_dates AS (
        select as2.id as symbol_id
            , as2.symbol
            , max(fbs.date_actual) as date_actual
            , max(fbs.date_id) as date_id
            from ticker_data.all_symbols as2
                left join ticker_data.fact_balance_sheet fbs
                    on as2.id = fbs.symbol_id 
                        group by as2.id

    )
    SELECT m.symbol_id, m.symbol, m.date_actual, dd.year_actual, dd.quarter_actual  
    FROM max_dates m
    left join dim_date dd
        on m.date_id = dd.date_dim_id
            order by 1;
''', engine).set_index('symbol_id')

balance_sheet_df_list = []
for row in balance_sheet_info.iterrows():
        symbol_id = row[0]
        symbol = row[1][0]
        last_date = row[1][1]
        last_year = row[1][2]
        last_quarter = row[1][3]

        temp_df = get_statement_delta(symbol_id=symbol_id, symbol=symbol, last_date=last_date, last_year=last_year
                , last_quarter=last_quarter, statement_type='balance-sheet-statement')
        balance_sheet_df_list.append(temp_df)

balance_sheets_df = pd.concat(balance_sheet_df_list).sort_values('link', ascending=False).drop_duplicates(['date_actual', 'symbol'])

balance_sheets_df.to_sql('fact_balance_sheet', engine, schema='ticker_data', if_exists='append', index=False)

print('Balance Sheet Download Complete')

# ----------------------------- cash flow statement ---------------------------------

print('Bginning Cash Flow Statement Download...')

cash_flow_statement_info = pd.read_sql('''
    WITH max_dates AS (
        select as2.id as symbol_id
            , as2.symbol
            , max(fcfs.date_actual) as date_actual
            , max(fcfs.date_id) as date_id
            from ticker_data.all_symbols as2
                left join ticker_data.fact_cash_flow_statement fcfs
                    on as2.id = fcfs.symbol_id 
                        group by as2.id

    )
    SELECT m.symbol_id, m.symbol, m.date_actual, dd.year_actual, dd.quarter_actual  
    FROM max_dates m
    left join dim_date dd
        on m.date_id = dd.date_dim_id
            order by 1;
''', engine).set_index('symbol_id')

cash_flow_statement_df_list = []
for row in cash_flow_statement_info.iterrows():
        symbol_id = row[0]
        symbol = row[1][0]
        last_date = row[1][1]
        last_year = row[1][2]
        last_quarter = row[1][3]

        temp_df = get_statement_delta(symbol_id=symbol_id, symbol=symbol, last_date=last_date, last_year=last_year
                , last_quarter=last_quarter, statement_type='cash-flow-statement')
        cash_flow_statement_df_list.append(temp_df)

cash_flow_statements_df = pd.concat(cash_flow_statement_df_list).sort_values('link', ascending=False).drop_duplicates(['date_actual', 'symbol'])

cash_flow_statements_df.to_sql('fact_cash_flow_statement', engine, schema='ticker_data', if_exists='append', index=False)

print('Cash Flow Statement Download Complete')
print('Statements Download Complete')