import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#module-urllib3.util.retry

import logging
logger = logging.getLogger(__name__)

def create_session():
    try:
        session = requests.Session()
        retry = Retry(total=5, connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('https://', adapter)
        return session
    except Exception as e:
        logger.error(e)

def get_api(session, url):
    try:
        r = session.get(url, headers={'Content-Type': 'application/json'})
        if r.status_code == requests.codes.ok: 
            return r.json()
    except Exception as e:
        logger.error(e)