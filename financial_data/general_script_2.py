from settings import *
import re
import requests
import pandas as pd

from table_ops.create_table import create_table
from table_ops.save_data import save_v2

import logging
logger = logging.getLogger(__name__)

class GeneralScriptTwo():

    def __init__(self, API_KEY, end_point):
        self.API_KEY, self.end_point = API_KEY, end_point
    
    def fetch_data(self):
        try:
            url = f"https://fmpcloud.io/api/v3/{self.end_point}?apikey={self.API_KEY}"
            r   = requests.get(url, headers={'Content-Type': 'application/json'})
            if r.status_code == requests.codes.ok: 
                data        = r.json()
                if data:
                    df = pd.DataFrame(data)
                    # convert column names to snake_case
                    df.rename(columns={col:self.camel_to_snake(col) for col in df.columns}, inplace=True)
                    return df.to_dict('records')
        
        except Exception as e:
            logger.error(e)

    def camel_to_snake(self, name):
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


if __name__ == '__main__':

    # fundamental tables
    args = [
        ('gainers', 'stock_market_performances', 'ticker')
    ]
    
    for arg in args:
        try:
            end_point, table_name, pk = arg

            logger.info('fetching data from endpoint %s'%(end_point))
            gs = GeneralScriptTwo(api_key, end_point)
            # create data from end_point
            values = gs.fetch_data()
            if values:
                # create table if not exists for end_point
                create_table(values, table_name, pk)
                # save data to table
                save_v2(values, table_name)
            
        except Exception as e:
            logger.error(e)