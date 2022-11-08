from settings import *
import re
import requests
import pandas as pd

from financial_data.symbols_exchange import get_symbols_exchanges
from table_ops.get_value import get_value
from table_ops.create_table import create_table
from table_ops.save_data import save

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


if __name__ == '__main__':

    # fundamental tables
    args = [
        ('income-statement', 'annual', 30),
        ('income-statement', 'quarter', 120),
        ('income-statement-growth', 'annual', 30),
        ('income-statement-growth', 'quarter', 120),
        ('balance-sheet-statement', 'annual', 30),
        ('balance-sheet-statement', 'quarter', 120),
        ('balance-sheet-statement-growth', 'annual', 30),
        ('balance-sheet-statement-growth', 'quarter', 120),
        ('cash-flow-statement', 'annual', 30),
        ('cash-flow-statement', 'quarter', 120),
        ('cash-flow-statement-growth', 'annual', 30),
        ('cash-flow-statement-growth', 'quarter', 120),
        ('ratios', 'annual', 30),
        ('ratios', 'quarter', 120),
        ('ratios-ttm', 'annual', 30),
        ('key-metrics-ttm', 'annual', 30),
        ('key-metrics', 'annual', 30),
        ('key-metrics', 'quarter', 120)
    ]
    
    for arg in args:
        try:
            end_point, period, limit = arg
            table_name = f"{period}_{end_point.replace('-','_')}"

            # get symbols and exchange data
            symbols, exchanges = get_symbols_exchanges(api_key, table_name)
            
            logger.info('fetching data from %s for %s companies for period %s'%(end_point, len(symbols), period))
            for symbol, exchange in zip(symbols, exchanges):
                currency = get_value(sql=" select currency from company_profile where symbol = '%s' "%symbol)
                if currency: currency = currency[0][0]
                else: currency = None
                ft = FundamentalTables(api_key, end_point, symbol, exchange, currency, period, limit)
                # create data from end_point
                values = ft.fetch_data()
                if values:
                    # create table if not exists for end_point
                    create_table(values, table_name)
                    # save data to table
                    save(values, table_name)
            
        except Exception as e:
            logger.error(e)