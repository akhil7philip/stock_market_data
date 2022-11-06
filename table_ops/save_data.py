import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
from settings import *

import logging
logger = logging.getLogger(__name__)

# save new values
def save(values, table_name, pk='tsid'):
    try:
        # filter for new values
        df = pd.DataFrame(values)
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute('SELECT %s from %s'%(pk,table_name))
        
        model_pk_set = set([val[0] for val in cur.fetchall()])
        sheet_pk_set = set(df[pk])
        pk_to_save = sheet_pk_set - model_pk_set

        # get new records from df
        l = [df[df.loc[:,pk] == val] for val in pk_to_save]
        if l:
            df = pd.concat(l, ignore_index=True)
            df['created_at'] = datetime.now()

            # save new values only to table
            engine = create_engine("postgresql://{user}:{password}@{host}/{database}".format(**conn_params))
            conn = engine.connect()
            df.to_sql(table_name, conn, if_exists='append', index=False)
            logger.info("saved %s new values for %s"%(len(df), table_name))
        else:
            logger.info("no new values to save for %s"%table_name)
    
    except Exception as e:
        logger.error(e)