import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
from settings import *

import logging
logger = logging.getLogger(__name__)

# save new values
def get_value(get_param, post_param, table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute("select %s from %s where symbol = '%s'"%(get_param, table_name, post_param))
        return cur.fetchone()[0]
    
    except Exception as e:
        logger.error(e)