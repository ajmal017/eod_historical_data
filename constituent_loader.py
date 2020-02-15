'''
https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/
https://eodhistoricaldata.com/knowledgebase/list-supported-indices/
https://eodhistoricaldata.com/knowledgebase/api-for-historical-data-and-volumes/
'''

import requests
import json
import pandas as pd
from datetime import datetime, timedelta

yday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

api = '5e16ffd41cd092.83534433'
s_date = '1970-12-31'
e_date = yday
index = 'GSPC'

# url = 'https://eodhistoricaldata.com/api/eod/AAPL.US?api_token=5e16ffd41cd092.83534433'
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

# df.loc[:, 'Code']
# unique_tickers = df.loc[:, 'Code'].unique()
# unique_tickers

