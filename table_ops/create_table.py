import sys
sys.path.insert(0,'/Users/akhil.philip/learn/upwork/stock_market_data')

from settings.settings import *
from numpy import dtype
import psycopg2
import pandas as pd
import logging
logger = logging.getLogger(__name__)

# create table
def create_table(values, table_name, pk='tsid'):
    try:
        df = pd.DataFrame(values)
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        s = "CREATE TABLE IF NOT EXISTS %s ("%table_name
        for k,v in df.dtypes.items():
            if k == 'date':
                v = 'DATE'
            elif k in {'filling_date','accepted_date'}:
                v = 'TIMESTAMP WITH TIME ZONE'
                # v = 'TIMESTAMP WITHOUT TIME ZONE'
            elif k == 'tsid':
                v = 'VARCHAR (30)'
            elif k in {'symbol','exchange_short_name'}:
                v = 'VARCHAR (10)'
            elif k == 'calendar_year':
                v = 'INTEGER'
            elif v in {dtype('int64'), dtype('float64')}:
                # v = 'DECIMAL(12,6)'
                v = 'NUMERIC'
            elif v in {dtype('O')}:
                v = 'TEXT'
            else:
                v = 'TEXT'
            s+=f'"{k}" {v}, '
        s+='created_at TIMESTAMP WITH TIME ZONE, PRIMARY KEY(%s))'%pk
        # s+='created_at TIMESTAMP WITHOUT TIME ZONE, PRIMARY KEY(%s))'%pk
        cur.execute(s)
        conn.commit()
        conn.close()

    except Exception as e:
        logger.error(e)