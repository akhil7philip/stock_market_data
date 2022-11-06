import re
import time
import requests
import pandas as pd

import logging
logger = logging.getLogger(__name__)

class FundamentalTables():

    def __init__(self, API_KEY, end_point, symbol, exchange, currency, period, limit):
        self.API_KEY, self.end_point, self.symbol, self.exchange, self.currency, self.period, self.limit = API_KEY, end_point, symbol, exchange, currency, period, limit
    
    def fetch_data(self):
        try:
            logger.info('fetching data from %s for %s symbol for period %s'%(self.end_point, self.symbol, self.period))
            url = f"https://fmpcloud.io/api/v3/{self.end_point}/{self.symbol}?period={self.period}&limit={self.limit}&apikey={self.API_KEY}"
            r   = requests.get(url, headers={'Content-Type': 'application/json'})
            if r.status_code == requests.codes.ok: 
                data        = r.json()
                if data:
                    df = pd.DataFrame(data)
                    # add 'symbol' if not exists
                    if 'symbol' not in df.columns:
                        df.insert(loc=0, column='symbol', value=self.symbol)
                    # add 'exchangeShortName' if not exists
                    if 'exchangeShortName' not in df.columns:
                        df.insert(2, 'exchangeShortName', self.exchange)
                    # add 'currency' if not exists
                    if 'currency' not in df.columns:
                        df.insert(2, 'currency', self.currency)
                    # add 'calendarYear' if not exists
                    if 'calendarYear' not in df.columns and self.end_point not in {'key-metrics-ttm','ratios-ttm','profile'}:
                        df.insert(3, 'calendarYear', df['date'].map(lambda x:x.split('-')[0]))
                    # add 'TSID'
                    if self.end_point in {'key-metrics-ttm','ratios-ttm','profile'}:
                        df.insert(0, 'TSID', df[['symbol','exchangeShortName']].apply(lambda x:'.'.join(x), axis=1))
                        count = len(df)
                    elif self.period == 'quarter':
                        df.insert(1, 'TSID', df[['symbol','exchangeShortName','calendarYear','period']].apply(lambda x:'.'.join(x), axis=1))
                        count = int(len(df)/4)
                    else:
                        df.insert(1, 'TSID', df[['symbol','exchangeShortName','calendarYear']].apply(lambda x:'.'.join(x), axis=1))
                        count = len(df)
                    # convert column names to snake_case
                    df.rename(columns={col:self.camel_to_snake(col) for col in df.columns}, inplace=True)
                    
                    logger.info('Receieved %s years of data for company %s'%(count, self.symbol))
                    return df.to_dict('records')
        
        except Exception as e:
            logger.error(e)

    def camel_to_snake(self, name):
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
