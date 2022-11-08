import requests
import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
from settings import *

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# get company names
def get_symbols_exchanges(API_KEY, table_name):
    '''
    To pull the symbols and exchange data via the fmpcloud api endpoint (stock).
    
    :param url: String url to fetch data from
    '''
    try:
        logger.info('fetching symbols and exchanges data')
        url = "https://fmpcloud.io/api/v3/stock/list?apikey=%s"%API_KEY
        r   = requests.get(url, headers={'Content-Type': 'application/json'})
        if r.status_code == requests.codes.ok: 
            data        = r.json()
            df = pd.DataFrame(data)
            # get only symbols from 'NASDAQ','NYSE' exchanges
            df = df.set_index('exchangeShortName').loc[['NASDAQ','NYSE'],'symbol'].reset_index()
            if table_name:
                # order symbols based on values from existing table
                conn = psycopg2.connect(**conn_params)
                cur = conn.cursor()
                cur.execute('SELECT distinct %s from %s'%('symbol',table_name))
                model_symbol_set = set([val[0] for val in cur.fetchall()])
                sheet_symbol_set = set(df['symbol'])
                symbol_to_save = list(sheet_symbol_set - model_symbol_set)
                symbol_to_save.extend(model_symbol_set)
                
                # get new records from df
                df = df.set_index('symbol').loc[symbol_to_save,'exchangeShortName'].reset_index()

            logger.info('fetched symbols and exchanges for %s companies'%len(df))
            return df['symbol'].values, df['exchangeShortName'].values
    
    except Exception as e:
        logger.error(e)