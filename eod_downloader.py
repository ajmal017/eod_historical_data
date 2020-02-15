import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2 as pg

api = '5e16ffd41cd092.83534433'
e_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

def eod_corp_act(sec, ex, corp_act_type, s_date = '1900-01-01'):
    '''
    Parameters
    ----------
    sec : string : security (eg. AAPL)
    ex : string : exchange (eg. US)
    corp_act_type: type of corporate action ('div', 'splits', 'shorts')
    s_date : string : 'yyyy-mm-dd' format

    Returns
    -------
    df : pandas dataframe 
    '''
    valid_types = ['div', 'splits', 'shorts']
    
    if corp_act_type in valid_types:

        url = (f'https://eodhistoricaldata.com/api/{corp_act_type}/'
               f'{sec}.{ex}?api_token={api}&from={s_date}&fmt=json')

        response = requests.get(url)
        data = response.text
        df = pd.read_json(data).T
        df.set_index('date', inplace = True)
    
        return df

    else:
        print('Not a valid corporate action type.')

def eod_etf(sec, ex, s_date = '1900-01-01'):
    '''
    Parameters
    ----------
    sec : string : security (eg. AAPL)
    ex : string : exchange (eg. US)
    s_date : string : 'yyyy-mm-dd' format

    Returns
    -------
    df : pandas dataframe 
    '''

    e_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    
    url = (f'https://eodhistoricaldata.com/api/fundamentals/{sec}.{ex}?'
           f'api_token={api}&historical=1&from={s_date}&to={e_date}')
    
    response = requests.get(url)
    data = response.text
    df = pd.read_json(data)

    return df

def eod_price(sec, ex, s_date = '1900-01-01'):
    '''
    Parameters
    ----------
    sec : string : security (eg. AAPL)
    ex : string : exchange (eg. US)
    s_date : string : 'yyyy-mm-dd' format

    Returns
    -------
    df : pandas dataframe 
    '''

    e_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    url = (f'https://eodhistoricaldata.com/api/eod/{sec}.{ex}?from={s_date}&to={e_date}'
           f'&api_token={api}&period=d&fmt=json')

    response = requests.get(url)
    data = response.text
    df = pd.read_json(data)
    df.set_index('date', inplace = True)
    
    return df
    
def eod_constituents(index, s_date = '1990-01-01'):
    
    url = (f'https://eodhistoricaldata.com/api/fundamentals/{index}.INDX?'
           f'api_token={api}&historical=1&from={s_date}&to={e_date}')
        
    
    response = requests.get(url)
    data = response.text
    df = pd.read_json(data)
    
    general_info = df['General'].dropna()
    constituents = df['Components'].dropna()
    
    if constituents.shape[0] > 0:
        constituent_keys = list(constituents[0].keys())
        constituent_values = [list(i.values()) for i in constituents]
        constituents = pd.DataFrame.from_records(constituent_values, columns = constituent_keys)
        
    return constituents, general_info



def eod_exchange_to_db(exchange_info):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    now = datetime.now().strftime('%Y-%m-%d')
    cols = 'short_name, name, country, created_date, last_updated_date' # won't need created_date everytime
    vals = f"'{exchange_info.Code}', '{exchange_info.Name}', '{exchange_info.CountryName}', '{now}', '{now}'"
    
    command = f'INSERT INTO exchange ({cols}) VALUES ({vals})' 

    cur.execute(command)
    con.commit()
    cur.close()
    con.close()
    
    
def eod_instruments_to_db(constituents, info):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    cur.execute('SELECT id FROM exchange')
    data = cur.fetchall()
    ex_id = data[-1][0]
    
    now = datetime.now().strftime('%Y-%m-%d')
    cols = ('exchange_id, ticker, instrument, name, sector, industry, '
            'currency, created_date, last_updated_date') # won't need created_date everytime
    
    currency = info['CurrencyCode']
    for index, row in constituents.iterrows():
        ticker = row['Code']
        name = row['Name'].replace("'", "")
        sector = row['Sector']
        industry = row['Industry']
        # exchange = row['Exchange']

        vals = (f"'{ex_id}', '{ticker}', 'equity', '{name}', '{sector}', "
                f"'{industry}', '{currency}', '{now}', '{now}'")
        command = f'INSERT INTO symbol ({cols}) VALUES ({vals})' 
        cur.execute(command)
    
    con.commit()
    cur.close()
    con.close()




if __name__ == '__main__':
    
    indices = pd.read_csv('eod_indices_2020_01.csv', names = ['type', 'symbol', 'name'])
    
    x = 0
    for index in indices['symbol']:
        percent_done = round((x / len(indices['symbol'])) * 100, 2)
        print(f'{percent_done}% complete. Working on {index}.')
        constituents, info = eod_constituents(index)
        if constituents.shape[0] > 0:
            eod_exchange_to_db(info)
            eod_instruments_to_db(constituents, info)
        x += 1
    print('Done.')



# 1 - import all exchanges from EOD make sure they are assigned a unique id
# 2 - import all instruments from those exchanges

# 2 - Set up the FTSE tickers in 'symbol'
#  Need to connect symbols with the index (')

# 3 - Import daily price data and load into 'daily_price'