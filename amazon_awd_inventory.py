import datetime as dt
from sp_api.base import Marketplaces
from sp_api.api import AmazonWarehousingAndDistribution
from sp_api.util import throttle_retry, load_all_pages
from sp_api.base.exceptions import SellingApiRequestThrottledException, SellingApiServerException
from requests.exceptions import ReadTimeout, ConnectionError

marketplace = 'US'
awd = AmazonWarehousingAndDistribution(account=marketplace, 
        marketplace=Marketplaces[marketplace], version='2024-05-09').list_inventory()


print(awd)

throttle_retry()
@load_all_pages()
def load_all_awd(marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    awd = AmazonWarehousingAndDistribution(account=marketplace, 
                            marketplace=Marketplaces[marketplace], version='2024-05-09')
    return awd