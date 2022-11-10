import sys
sys.path.insert(0,'/Users/akhil.philip/learn/upwork/stock_market_data')

from settings.settings import *
import re
import requests
import pandas as pd

from financial_data.symbols_exchange import get_symbols_exchanges
from helper_funcs.get_api import get_api, create_session
from table_ops.create_table import create_table
from table_ops.save_data import save

import logging
logger = logging.getLogger(__name__)

class GeneralScriptOne():

    def __init__(self, API_KEY, session, end_point, symbol, exchange, period, limit):
        self.API_KEY, self.session, self.end_point, self.symbol, self.exchange, self.period, self.limit = API_KEY, session, end_point, symbol, exchange, period, limit
    
    def fetch_data(self):
        try:
            logger.info('fetching data from %s for %s symbol for period %s'%(self.end_point, self.symbol, self.period))
            url = f"https://fmpcloud.io/api/v3/{self.end_point}/{self.symbol}?period={self.period}&limit={self.limit}&apikey={self.API_KEY}"
            data = get_api(self.session, url)
            if data:
                df = pd.DataFrame(data)
                # add 'symbol' if not exists
                if 'symbol' not in df.columns:
                    df.insert(loc=0, column='symbol', value=self.symbol)
                # add 'exchangeShortName' if not exists
                if 'exchangeShortName' not in df.columns:
                    df.insert(2, 'exchangeShortName', self.exchange)
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


if __name__ == '__main__':

    # fundamental tables
    args = [
        ('profile','annual', 30, 'company_profile')
    ]
    
    for arg in args:
        try:
            end_point, period, limit, table_name = arg

            # get symbols and exchange data
            symbols, exchanges = get_symbols_exchanges(api_key, table_name)

            session = create_session()
            logger.info('fetching data from %s for %s companies for period %s'%(end_point, len(symbols), period))
            for symbol, exchange in zip(symbols, exchanges):
                gs = GeneralScriptOne(api_key, session, end_point, symbol, exchange, period, limit)
                # create data from end_point
                values = gs.fetch_data()
                if values:
                    # create table if not exists for end_point
                    create_table(values, table_name)
                    # save data to table
                    save(values, table_name)
            
        except Exception as e:
            logger.error(e)