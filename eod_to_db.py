import psycopg2 as psy
import requests
import json
import pandas as pd
from datetime import datetime, timedelta

yday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

api = '5e16ffd41cd092.83534433'
s_date = '1970-12-31'
e_date = yday
index = 'GSPC'

url = f'https://eodhistoricaldata.com/api/fundamentals/{index}.INDX?api_token={api}&historical=1&from={s_date}&to={e_date}'

response = requests.get(url)
data = response.text
parsed = json.loads(data)

# For most recent constituents only
recent_df = pd.DataFrame(columns = parsed['Components']['0'].keys())

dates = list(parsed['HistoricalComponents'].keys())

df = pd.DataFrame.from_dict({(i, j): parsed['HistoricalComponents'][i][j]
                        for i in parsed['HistoricalComponents'].keys()
                        for j in parsed['HistoricalComponents'][i].keys()},
                       orient = 'index')

# df.index[0][1] # 

# connect to the db
con = psy.connect(
    database = 'securities_master', 
    user = 'postgres', 
    )

table = 'sp500_constituents'
cols = 'date, ticker, name, sector, industry, exchange'

# cursor
cur = con.cursor()

for index, row in df.iterrows():
    ticker = row['Code']
    date = row['Date']
    name = row['Name'].replace("'", "")
    sector = row['Sector']
    industry = row['Industry']
    exchange = row['Exchange']
    
    cur.execute(f"INSERT INTO {table} ({cols}) "
                f"VALUES ('{date}', '{ticker}', '{name}', '{sector}', '{industry}', '{exchange}')")

# commit changes to the db
con.commit()
    
# close the cursor
cur.close()
# close the connection
con.close()