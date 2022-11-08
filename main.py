from settings import *
from table_ops.create_table import create_table
from table_ops.save_data import save
from table_ops.get_value import get_value
from financial_data.symbols_exchange import get_symbols_exchanges
from financial_data.fundamental_tables import FundamentalTables
from financial_data.price_volume_tabes import PriceVolumeTables

import logging
logger = logging.getLogger(__name__)

class DataFlow():

    def __init__(self, API_KEY, end_point, period, limit):
        self.API_KEY = API_KEY
        self.end_point = end_point
        self.period = period
        self.limit = limit
    
    def data_flow(self):
        try:
            # get symbols and exchange data
            symbols, exchanges = get_symbols_exchanges(self.API_KEY)
            
            # for price and volume tables
            if self.end_point in ('historical-price-full'):
                symbols = symbols[:6]
                pvt = PriceVolumeTables(self.API_KEY, self.end_point, symbols, self.period, self.limit)
                # create data from end_point
                price_vals, volume_vals = pvt.fetch_data()
                if price_vals:
                    # create table if not exists for end_point
                    create_table(price_vals, "daily_price_per_ticker", pk='date')
                    create_table(volume_vals, "daily_volume_per_ticker", pk='date')
                    # save data to table
                    save(price_vals, "daily_price_per_ticker", pk='date')
                    save(volume_vals, "daily_volume_per_ticker", pk='date')

            # for fundamental tables
            else:        
                logger.info('fetching data from %s for %s companies for period %s'%(self.end_point, len(symbols), self.period))
                for symbol, exchange in zip(symbols, exchanges):
                    currency = get_value(sql=" select currency from company_profile where symbol = '%s' "%symbol)[0][0]
                    ft = FundamentalTables(self.API_KEY, self.end_point, symbol, exchange, currency, self.period, self.limit)
                    # create data from end_point
                    values = ft.fetch_data()
                    if values:
                        table_name = f"{self.period}_{self.end_point.replace('-','_')}"
                        # create table if not exists for end_point
                        create_table(values, table_name)
                        # save data to table
                        save(values, table_name)
            
        except Exception as e:
            logger.error(e)

if __name__ == '__main__':

    args = [
        # fundamental tables
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

        # price and volume tables
        ('historical-price-full', 'annual', 30),
    ]
    
    for arg in args:
        o = DataFlow(api_key, *arg)
        o.data_flow()