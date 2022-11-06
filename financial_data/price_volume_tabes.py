import re
import time
import requests
import pandas as pd

import logging
logger = logging.getLogger(__name__)

class PriceVolumeTables():

    def __init__(self, API_KEY, end_point, symbols, period, limit):
        self.API_KEY, self.end_point, self.symbols, self.period, self.limit = API_KEY, end_point, symbols, period, limit
    
    def fetch_data(self):
        try:
            final_price = pd.DataFrame(columns=['date'])
            final_volume = pd.DataFrame(columns=['date'])
            for symbol in self.symbols:
                logger.info('fetching data from %s for %s symbol for period %s'%(self.end_point, symbol, self.period))
                url = f"https://fmpcloud.io/api/v3/{self.end_point}/{symbol}?period={self.period}&limit={self.limit}&apikey={self.API_KEY}"
                r   = requests.get(url, headers={'Content-Type': 'application/json'})
                if r.status_code == requests.codes.ok: 
                    data        = r.json()
                    if data:
                        # Daily Price per Ticker
                        temp_price  = pd.json_normalize(data, record_path='historical', meta=['symbol']).rename(columns={'close':symbol})[['date',symbol]]
                        final_price = final_price.merge(temp_price, on='date', how='outer').sort_values('date',ascending=False)

                        # Daily Price per Ticker
                        temp_volume = pd.json_normalize(data, record_path='historical', meta=['symbol']).rename(columns={'volume':symbol})[['date',symbol]]
                        final_volume= final_volume.merge(temp_volume, on='date', how='outer').sort_values('date',ascending=False)
        
            # convert column names to snake_case
            final_price.rename(columns={col:self.camel_to_snake(col) for col in final_price.columns}, inplace=True)
            final_volume.rename(columns={col:self.camel_to_snake(col) for col in final_volume.columns}, inplace=True)
            
            logger.info('Receieved %s companies price and volume records'%final_price.shape[1])
            return final_price.to_dict('records'), final_volume.to_dict('records')

        except Exception as e:
            logger.error(e)

    def camel_to_snake(self, name):
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()