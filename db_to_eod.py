import psycopg2 as psy
import requests
import json
import pandas as pd
from datetime import datetime, timedelta


# connect to the db
con = psy.connect(
    database = 'securities_master', 
    user = 'postgres', 
    )

# cursor
cur = con.cursor()

# cur.execute("INSERT INTO person (person_uid, first_name, last_name, email, gender, "
#            "date_of_birth, country_of_birth) VALUES (uuid_generate_v4(), 'Hugo', 'Roxbee', "
#            "'hroxbee1x@home.pl', 'Male', '2019-03-30', 'Spain')")

# execute query
cur.execute('SELECT DISTINCT ticker FROM sp500_constituents')

rows = cur.fetchall()

tickers = [tick[0] for tick in rows]

# commit changes to the db
# con.commit()
    
# close the cursor
cur.close()
# close the connection
con.close()

parsed[0]

yday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

api = '5e16ffd41cd092.83534433'
s_date = '1969-12-31'
# e_date = '2019-12-31'
e_date = yday
sec = 'SPY'
exchg = 'US'
req_type = 'eod'

if req_type == 'fundamentals':
    url = f'https://eodhistoricaldata.com/api/{req_type}/{sec}.{exchg}?api_token={api}&historical=1&from={s_date}&to={e_date}'
elif req_type == 'eod':
    url = f'https://eodhistoricaldata.com/api/{req_type}/{sec}.{exchg}?from={s_date}&to={e_date}&api_token={api}&period=d&fmt=json'

response = requests.get(url)
data = response.text
parsed = json.loads(data)

parsed[0]['date']

for row in parsed: 
    d = parsed[row]['date']
    o = parsed[row]['open']
    h = parsed[row]['high']
    l = parsed[row]['low']
    c = parsed[row]['close']
    ac = parsed[row]['adjusted_close']
    v = parsed[row]['volume']












# for ticker in tickers:
    # 
