import logging
from ad_api.api import Profiles
from ad_api.base import AdvertisingApiException
from sp_api.base import Marketplaces


def list_profiles(account, marketplace, **kwargs):
    # Issue: Always defaults to EU marketplace
    logging.info("-------------------------------------")
    logging.info("Profiles > list_profiles(%s)" % kwargs)
    logging.info("-------------------------------------")

    try:

        result = Profiles(account=f'{account}-{marketplace}',
                        #   marketplace=Marketplaces[marketplace],
                          debug=True).list_profiles(**kwargs)
        logging.info(result)

        accounts_info = result.payload

        for account_info in accounts_info:
            logging.info(account_info)

    except AdvertisingApiException as error:
        logging.info(error)


def get_profile(account, marketplace, profile_id: int):

    logging.info("-------------------------------------")
    logging.info("Profiles > get_profile(%s)" % profile_id)
    logging.info("-------------------------------------")

    try:

        result = Profiles(account=f'{account}-{marketplace}', 
                          debug=True).get_profile(profileId=profile_id)
        logging.info(result)

        profile_info = result.payload

    except AdvertisingApiException as error:
        logging.info(error)

if __name__ == '__main__':
    list_profiles('Rymora', 'UK', profileTypeFilter="seller")