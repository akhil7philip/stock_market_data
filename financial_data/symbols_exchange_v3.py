import sys
sys.path.insert(0,'/Users/akhil.philip/learn/upwork/stock_market_data')

from settings.settings import *
import pandas as pd
from helper_funcs.get_api import get_api, create_session
from table_ops.table_ops import get_value
import logging
logger = logging.getLogger(__name__)

# for price and volumes table
def get_symbols_exchanges(API_KEY, table_name, port=5432):
    '''
    To pull the symbols and exchange data via the fmpcloud api endpoint (stock).
    
    :param url: String url to fetch data from
    '''
    try:
        limit = 1000
        logger.info('fetching symbols and exchanges data')
        url = "https://fmpcloud.io/api/v3/stock/list?apikey=%s"%API_KEY
        session = create_session()
        data = get_api(session, url)
        if data:
            df = pd.DataFrame(data)
            # get only symbols from 'NASDAQ','NYSE' exchanges
            df = df.set_index('exchangeShortName').loc[['NASDAQ','NYSE'],'symbol'].reset_index()
            l = [0, 1500, 3000, 4500, 6000, 7500, 9000, len(df)]
            for count, table_name in enumerate([
                'daily_price_per_ticker_1',
                'daily_price_per_ticker_2',
                'daily_price_per_ticker_3',
                'daily_price_per_ticker_4',
                'daily_price_per_ticker_5',
                'daily_price_per_ticker_6',
                'daily_price_per_ticker_7']):
                try:
                    # order symbols based on values from existing table;
                    # i.e create a list of symbols that don't exist in database first, 
                    # and then append the remaining symbols to that list
                    s = "SELECT "
                    for symbol in df['symbol'].values[l[count]:l[count+1]]:
                        s += 'max("%s"), '%(symbol)
                    s += 'max(date) from %s t'%table_name
                    values = get_value(s, port=port)
                    if values: 
                        values = values[0]
                        try:
                            i = values.index(None)
                            limit = 1000
                            print("index at: %s"%i)
                            df = df.iloc[
                                range(
                                    i+1500*count,
                                    len(df)
                                    )]
                            break
                        except:
                            logger.info("all values filled for table %s"%table_name)
                except:
                    logger.info("table %s doesn't exist"%table_name)

            logger.info('fetched symbols and exchanges for %s companies'%len(df))
            return df['symbol'].values, limit
    
    except Exception as e:
        logger.error(e)