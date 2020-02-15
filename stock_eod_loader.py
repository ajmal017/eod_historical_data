import pandas as pd
import requests
import json
# from pandas.compat import StringIO

api = '5e16ffd41cd092.83534433'

def get_eod_data(symbol, session = None):
    
    if session is None: 
        session = requests.Session()
    
    url = f'https://eodhistoricaldata.com/api/eod/{symbol}'
    params = {'api_token': api}
    r = session.get(url, params = params)
    
    if r.status_code == requests.codes.ok:
        data = json.loads(r.text)
        # df = pd.read_csv(StringIO(r.text), skipfooter = 1, 
                         # parse_dates = [0], index_col = 0)
   
        return data
        # return df
    
    else:
    
        raise Exception(r.status_code, r.reason, url)

    

get_eod_data('AAPL.US')