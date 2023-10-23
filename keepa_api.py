import keepa
import json
import postgresql
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open('config.json') as f:
    config = json.load(f)
    api_key = config['keepa_key']

api = keepa.Keepa(api_key)

asin = 'B0BKH14N5N'
products = api.query(asin, offers=True)
product = products[0]