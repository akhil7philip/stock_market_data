import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
from settings import *

import logging
logger = logging.getLogger(__name__)

# save new values
def get_value(sql):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    
    except Exception as e:
        logger.error(e)