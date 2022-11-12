import sys
sys.path.insert(0,'/Users/akhil.philip/learn/upwork/stock_market_data')

from settings.settings import *
import psycopg2

import logging
logger = logging.getLogger(__name__)


# save new values
def get_value(sql):
    """
    pass query in parameter 'sql' to return result from table in db 
    """
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    
    except Exception as e:
        logger.error(e)


# set value
def set_value(sql):
    """
    pass query in parameter 'sql' to commit result from table in db 
    """
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    
    except Exception as e:
        logger.error(e)