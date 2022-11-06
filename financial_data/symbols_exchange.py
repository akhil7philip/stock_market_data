import requests
import pandas as pd

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# get company names
def get_symbols_exchanges(API_KEY):
    try:
        logger.info('fetching symbols and exchanges data')
        url = "https://fmpcloud.io/api/v3/stock/list?apikey=%s"%API_KEY
        r   = requests.get(url, headers={'Content-Type': 'application/json'})
        if r.status_code == requests.codes.ok: 
            data        = r.json()
            logger.info('fetched symbols and exchanges for %s companies'%len(data))
            df = pd.DataFrame(data)
            return df['symbol'].values, df['exchangeShortName'].values
            
    except Exception as e:
        logger.error(e)