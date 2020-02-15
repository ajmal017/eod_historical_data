import psycopg2 as pg
import requests
import json
# import pandas as pd
from datetime import datetime, timedelta

# connect to the db
con = pg.connect(
    database = 'securities_master', 
    user = 'postgres', 
    )

cur = con.cursor()

yday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

def obtain_list_of_db_tickers():
    '''
    Obtains a list of the ticker symbols in the db
    '''
    # cursor
    # cur = con.cursor()
    
    command = """select symbol.id, symbol.ticker, exchange.short_name
            from symbol join exchange on (symbol.exchange_id = exchange.id)
            where short_name = 'FTLC'"""
    
    cur.execute(command)
    
    data = cur.fetchall()
    return [(d[0], d[1]) for d in data]

def get_daily_historic_data_eod(ticker, 
                                exchange,
                                start_date = '2000-1-1',
                                end_date = yday):
    '''
    Obtains data from EOD Historical Data returns list of tuples
    
    ticker: EOD ticker, eg AAPL.US
    start_date: 'YYYY-M-D' 
    end_date: 'YYYY-M-D' 
    '''
    
    api = '5e16ffd41cd092.83534433'
    sec = ticker
    req_type = 'eod'
    
    if req_type == 'fundamentals':
        url = f'https://eodhistoricaldata.com/api/{req_type}/{sec}.{exchange}?api_token={api}&historical=1&from={start_date}&to={end_date}'
    elif req_type == 'eod':
        url = f'https://eodhistoricaldata.com/api/{req_type}/{sec}.{exchange}?from={start_date}&to={end_date}&api_token={api}&period=d&fmt=json'
    
    response = requests.get(url)
    data = response.text
    prices= json.loads(data)
    
    return prices

def insert_daily_data_into_db(data_vendor_id, symbol_id, daily_data):
    '''
    Enters the daily price data to the db. Appends the vendor_id and 
    symbol_id to the data. 
    
    daily_data: ......
    '''
    
    
    # create the time now
    now = datetime.now().strftime('%Y-%m-%d')
    
    # amend the data to include the vendor_id and the symbol_id
    daily_data = [(data_vendor_id, symbol_id, d['date'], now, now, d['open'], 
                   d['high'], d['low'], d['close'], d['volume'], 
                   d['adjusted_close']) for d in daily_data]
    
    # print(daily_data[0])
    
    # d = daily_data['date']
    # o = daily_data['open']
    # h = daily_data['high']
    # l = daily_data['low']
    # c = daily_data['close']
    # ac = daily_data['adjusted_close']
    # v = daily_data['volume']
    
    cols = ('data_vendor_id, symbol_id, price_date, created_date, '
            'last_updated_date, open_price, high_price, low_price, '
            'close_price, volume, adj_close_price')
    # vals = f"'{data_vendor_id}', '{symbol_id}', '{d}', 'now', 'now', '{o}', '{h}', '{l}', '{c}', '{v}', '{ac}'"
    
    vals = ("%s, " * 11)[:-2]
    
    command = 'INSERT INTO daily_price (%s) VALUES (%s)' % (cols, vals)
    
    # cur = con.cursor()
    cur.executemany(command, daily_data)    
    
if __name__ == '__main__':
    
    tickers = obtain_list_of_db_tickers()
    last_num = tickers[-1][0]
    tickers = [(last_num + 1, 'SPY'), (last_num + 2, 'QQQ')]
    for tick in tickers:
        print(f'Adding data for {tick}')
        eod_data = get_daily_historic_data_eod(tick[1], 'US', start_date = '1900-1-1')
        insert_daily_data_into_db(1, tick[0], eod_data)


con.commit()
cur.close()
con.close()
    