from sp_api.api import FulfillmentInbound
from sp_api.base import Marketplaces, SellingApiException, SellingApiBadRequestException, SellingApiNotFoundException
from sp_api.util import throttle_retry, load_all_pages
import datetime as dt
from utility import to_list
import pandas as pd
import time
import postgresql
import logging
import logger_setup


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

tenants = postgresql.get_tenants()


@throttle_retry()
@load_all_pages(extras=dict(QueryType='NEXT_TOKEN'))
def load_all_shipments(account='Bare Barrel', marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    response = FulfillmentInbound(
                            account=f'{account}-{marketplace}',
                            marketplace=Marketplaces[marketplace]
                            ).get_shipments(**kwargs)
    return response


@throttle_retry()
@load_all_pages(extras=dict(QueryType='NEXT_TOKEN'))
def load_all_shipment_items(account='Bare Barrel', marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    response = FulfillmentInbound(
                            account=f'{account}-{marketplace}',
                            marketplace=Marketplaces[marketplace]
                            ).shipment_items(**kwargs)
    return response


@throttle_retry()
@load_all_pages(next_token_param='paginationToken')
def load_all_inbound_plans(account='Bare Barrel', marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    response = FulfillmentInbound(
                            account=f'{account}-{marketplace}',
                            marketplace=Marketplaces[marketplace],
                            version='2024-03-20'
                            ).list_inbound_plans(**kwargs)
    return response


def get_all_shipments(account='Bare Barrel', marketplaces=['US', 'UK'], **kwargs):
    """
    Gets & combines all inbound shipments top level details. Uses version v0.
    Marketplaces are aggregated by region. E.g. US & CA are the the same.
    https://developer-docs.amazon.com/sp-api/docs/fulfillment-inbound-api-v0-reference#getshipments

    Args:
        ShipmentStatusList (str|array): WORKING,READY_TO_SHIP,SHIPPED,IN_TRANSIT,DELIVERED,CHECKED_IN,RECEIVING,CLOSED,CANCELLED,DELETED,ERROR
        ShipmentIdList (str|array): list of shipment ids
        LastUpdatedAfter (str|date)
        LastUpdatedBefore (str|date)
        QueryType (str): SHIPMENT (Default) - Returns shipments based on the shipment information provided by the ShipmentStatusList or ShipmentIdList parameters.
                            DATE_RANGE - Returns shipments based on the date range information provided by the LastUpdatedAfter and LastUpdatedBefore parameters.
                            NEXT_TOKEN	- Returns shipments by using NextToken to continue returning items specified in a previous request.
        MarketplaceId (str): Required.

    Returns:
        df
    """
    shipments_data = pd.DataFrame()

    for marketplace in to_list(marketplaces):
        logger.info(f'Retrieving Fulfillment Inbound Shipments from {account}-{marketplace} \n {kwargs}')
        response = load_all_shipments(account, marketplace, **kwargs)

        page_no = 0
        for page in response:
            page_no += 1
            logger.info(f"\tProcessing Page {page_no}. . .")
            payload = page.payload.get('ShipmentData')
            data = pd.json_normalize(payload)
            # data.insert(0, 'marketplace', marketplace) # Marketplaces are aggregated by region
            data['tenant_id'] = tenants[account]
            shipments_data = pd.concat([shipments_data, data], ignore_index=True)
            time.sleep(0.5)

    return shipments_data


def get_all_shipment_items(account='Bare Barrel', marketplaces=['US', 'UK'], **kwargs):
    """
    Gets & combines all inbound shipments items. Uses version v0.
    Marketplaces are aggregated by region. E.g. US & CA are the the same.
    https://developer-docs.amazon.com/sp-api/docs/fulfillment-inbound-api-v0-reference#getshipmentitems


    Args:
        ShipmentId (str): Retrieve all shipment items of a shipment
        LastUpdatedAfter (str|date)
        LastUpdatedBefore (str|date)
        QueryType (str): SHIPMENT (Default) - Returns shipments based on the shipment information provided by the ShipmentStatusList or ShipmentIdList parameters.
                            DATE_RANGE - Returns shipments based on the date range information provided by the LastUpdatedAfter and LastUpdatedBefore parameters.
                            NEXT_TOKEN	- Returns shipments by using NextToken to continue returning items specified in a previous request.

    Returns:
        df
    """
    shipments_data = pd.DataFrame()

    for marketplace in to_list(marketplaces):
        logger.info(f'Retrieving Fulfillment Inbound Shipment Items from {account}-{marketplace} \n {kwargs}')
        response = load_all_shipment_items(account, marketplace, **kwargs)

        page_no = 0
        for page in response:
            page_no += 1
            logger.info(f"\tProcessing Page {page_no}")
            payload = page.payload.get('ItemData')
            data = pd.json_normalize(payload)
            # data.insert(0, 'marketplace', marketplace) # Marketplaces are aggregated by region
            data['tenant_id'] = tenants[account]
            shipments_data = pd.concat([shipments_data, data], ignore_index=True)
            time.sleep(0.5)

    return shipments_data



def get_all_inbound_plans(account='Bare Barrel', marketplaces=['US', 'UK'], statuses=['ACTIVE', 'VOIDED', 'SHIPPED'], **kwargs):
    """
    Provides a list of inbound plans with minimal information.
    Gets and combined all inbound plans. Uses version 2024-03-20.
    Marketplaces are aggregated by region. E.g. US & CA are the the same.
    https://developer-docs.amazon.com/sp-api/docs/fulfillment-inbound-api-v2024-03-20-reference#listinboundplans

    Args:
        pageSize (int): The number of inbound plans to return in the response matching the given query. 
                        Min 10 (default) Max 30.
        paginationToken (str): A token to fetch a certain page when there are multiple pages worth of results.
                                The value of this token is fetched from the pagination return in the API response.
                                In the absence of the token value from the query parameter the API returns the first page of the result.
        statuses (str|list): ACTIVE (default) - An inbound plan that is being worked on.
                        VOIDED - An inbound plan with all shipment cancelled and can no longer be modified.
                        SHIPPED - A complete inbound plan. Only minor modifications can be made at this time.
        sortBy (str): LAST_UPDATED_TIME, CREATION_TIME
        sortOrder (str): ASC, DESC

    Returns:
        df
    """
    inbound_data = pd.DataFrame()

    for marketplace in to_list(marketplaces):

        for status in to_list(statuses):
            logger.info(f'Retrieving Inbound Plans ({status}) from {account}-{marketplace} \n {kwargs}')

            response = load_all_inbound_plans(account, marketplace, status=status, **kwargs)

            page_no = 0
            for page in response:
                page_no += 1
                logger.info(f"\tProcessing Page {page_no}. . .")
                payload = page.payload.get('inboundPlans')
                data = pd.json_normalize(payload)
                # data.insert(0, 'marketplace', marketplace) # Marketplaces are aggregated by region
                data['tenant_id'] = tenants[account]
                inbound_data = pd.concat([inbound_data, data], ignore_index=True)
                time.sleep(0.5)

    inbound_data.rename(columns={'createdAt': 'date_created_at'}, inplace=True)

    return inbound_data


def get_all_inbound_plans_info(account='Bare Barrel', marketplaces=['US', 'UK'], inbound_plan_ids='All', last_updated_at='1900-01-01'):
    """
    Fetches and combines the top level information of inbound plans.
    The shipment_id inside shipments can used to retrieve FBA shipment details in getShipment.
    It can query all inbound_plan_ids from the database.
    https://developer-docs.amazon.com/sp-api/docs/fulfillment-inbound-api-v2024-03-20-reference#getinboundplan

    Args:
        inbound_plan_ids (str|list): inbound_plan_id. If set to 'All', it would query fulfillment_inbound.inbound_plans
        last_updated_at (str|date)
    
    Returns:
        df
    """
    inbound_data = pd.DataFrame()

    for marketplace in to_list(marketplaces):
        logger.info(f"Fetching top level information of inbound plans from {account}-{marketplace}")

        if inbound_plan_ids == 'All':
            logger.info(f"Querying inbound_plan_ids from the database where last_updated_at >= {last_updated_at}. . .")
            query = postgresql.sql_to_dataframe(f"""
                                                SELECT inbound_plan_id 
                                                FROM fulfillment_inbound.inbound_plans
                                                WHERE marketplace_ids @> ARRAY['{Marketplaces[marketplace].marketplace_id}']
                                                    AND last_updated_at >= '{last_updated_at}'
                                                    AND tenant_id = {tenants[account]};
                                                """, 
            )
            inbound_plan_ids = list(query['inbound_plan_id'])

        logger.info(f"\tTotal {len(inbound_plan_ids)} inbound plan ids")

        for inbound_plan_id in to_list(inbound_plan_ids):
            logger.info(f"\tGetting inbound_plan_id: {inbound_plan_id}")

            try:
                response = FulfillmentInbound(
                                account=f'{account}-{marketplace}',
                                marketplace=Marketplaces[marketplace],
                                version='2024-03-20'
                                ).get_inbound_plan(inboundPlanId=inbound_plan_id)
                payload = response.payload
                data = pd.json_normalize(payload)
                data['tenant_id'] = tenants[account]
                inbound_data = pd.concat([inbound_data, data], ignore_index=True)

            # Doesn't currently support AWD or cross border Amazon Global Logistics
            # How to determine if it's from AWD or AGL???
            except (SellingApiBadRequestException, SellingApiNotFoundException, SellingApiException) as error:
                logger.warning(error)

    inbound_data.rename(columns={'createdAt': 'date_created_at'}, inplace=True)

    return inbound_data
    

def get_all_inbound_plans_shipments(account='Bare Barrel', inbound_plan_ids='All', last_updated_at='01-01-1970'):
    """
    Fetches & combines the full details for a specific shipment within an inbound plan.
    Queries database to retrieve shipment_ids and marketplace. Currently, only available in NA.
    https://developer-docs.amazon.com/sp-api/docs/fulfillment-inbound-api-v2024-03-20-reference#getshipment

    Args:
        inbound_plan_ids (str|list)
        last_updated_at (str|date)

    Returns:
        df
    """
    logger.info(f"Fetching shipments of inbound plans {inbound_plan_ids} where last_updated_at >= {last_updated_at}")

    query = f"""
                SELECT t1.inbound_plan_id,
                    t2.marketplace_name marketplace,
                    jsonb_array_elements(t1.shipments)->>'shipmentId' shipment_id
                FROM fulfillment_inbound.inbound_plans_info t1
                LEFT JOIN amazon_marketplaces t2 ON t1.marketplace_ids[1] = t2.marketplace_id
                WHERE t1.last_updated_at >= '{last_updated_at}'
                    AND t1.tenant_id = {tenants[account]}
            """

    if inbound_plan_ids != 'All':
        query += " AND inbound_plan_id = ANY (%s); ", (to_list(inbound_plan_ids), )

    # Queries each inbound_plan_id, shipment_id and marketplace_id
    df = postgresql.sql_to_dataframe(query, (to_list(inbound_plan_ids), ) )

    inbound_shipments_data = pd.DataFrame()

    for index, row in df.iterrows():
        inbound_plan_id, shipment_id, marketplace = row['inbound_plan_id'], row['shipment_id'], row['marketplace']

        logger.info(f"\tGetting {marketplace} shipments for {inbound_plan_id} - {shipment_id}")

        response = FulfillmentInbound(
                        account=f'{account}-{marketplace}',
                        marketplace=Marketplaces[marketplace],
                        version='2024-03-20'
                        ).get_shipment(inbound_plan_id, shipment_id)
        payload = response.payload
        data = pd.json_normalize(payload)
        data['tenant_id'] = tenants[account]
        inbound_shipments_data = pd.concat([inbound_shipments_data, data], ignore_index=True)

    return inbound_shipments_data


def update_data(table_name, start_date=None, end_date=None, account='Bare Barrel', marketplaces=['US', 'UK']):
    """
    Checks for last X days of updated data.
    Upserts fulfillment inbound daily.
    """
    if not start_date and not end_date:
        end_date = dt.date.today() + dt.timedelta(days=1)
        start_date = dt.date.today() - dt.timedelta(days=7)

    table_names = {
        'fulfillment_inbound.shipments': lambda: get_all_shipments(
                                            account,
                                            marketplaces,
                                            LastUpdatedAfter=start_date, 
                                            LastUpdatedBefore=end_date,
                                            ShipmentStatusList='WORKING,READY_TO_SHIP,SHIPPED,IN_TRANSIT,DELIVERED,CHECKED_IN,RECEIVING,CLOSED,CANCELLED,DELETED,ERROR',
                                            QueryType='DATE_RANGE'
                                            ),
        'fulfillment_inbound.shipment_items': lambda: get_all_shipment_items(
                                                account,
                                                marketplaces,
                                                LastUpdatedAfter=start_date, 
                                                LastUpdatedBefore=end_date, 
                                                QueryType='DATE_RANGE'
                                                ),
        'fulfillment_inbound.inbound_plans': lambda: get_all_inbound_plans(
                                                        account,
                                                        marketplaces,
                                                        pageSize=30,
                                                        sortBy='CREATION_TIME'
                                                ),
        'fulfillment_inbound.inbound_plans_info': lambda: get_all_inbound_plans_info(
                                                            account,
                                                            last_updated_at=start_date
                                                ),
        'fulfillment_inbound.inbound_plans_shipments': lambda: get_all_inbound_plans_shipments(
                                                                account,
                                                                inbound_plan_ids='All',
                                                                last_updated_at=start_date
        )
    }
    data = table_names[table_name]()
    logger.info(f"Upserting table {table_name} {start_date} - {end_date}")
    postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def create_table(table_name, drop_table_if_exists=False):
    table_names = {
        'fulfillment_inbound.shipments': lambda: get_all_shipments(
                                            LastUpdatedAfter='2022-01-01', 
                                            LastUpdatedBefore=dt.date.today(),
                                            ShipmentStatusList='WORKING,READY_TO_SHIP,SHIPPED,IN_TRANSIT,DELIVERED,CHECKED_IN,RECEIVING,CLOSED,CANCELLED,DELETED,ERROR',
                                            QueryType='DATE_RANGE'
                                            ),
        'fulfillment_inbound.shipment_items': lambda: get_all_shipment_items(
                                                LastUpdatedAfter='2022-01-01', 
                                                LastUpdatedBefore=dt.date.today(), 
                                                QueryType='DATE_RANGE'
                                                ),
        'fulfillment_inbound.inbound_plans': lambda: get_all_inbound_plans(
                                                pageSize=30,
                                                sortBy='CREATION_TIME'
                                                ),
        'fulfillment_inbound.inbound_plans_info': lambda: get_all_inbound_plans_info(),
        'fulfillment_inbound.inbound_plans_shipments': lambda: get_all_inbound_plans_shipments('All')
    }

    data = table_names[table_name]()

    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        postgresql.create_table(cur, data, file_extension='pandas', table_name=table_name)

        postgresql.update_updated_at_trigger(cur, table_name)

        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


if __name__ == '__main__':
    table_names = [
                  'fulfillment_inbound.shipments', 
                  'fulfillment_inbound.shipment_items',
                  'fulfillment_inbound.inbound_plans',
                  'fulfillment_inbound.inbound_plans_info',
                  'fulfillment_inbound.inbound_plans_shipments'
                   ]
    
    for account in tenants.keys():
        for table_name in table_names:
            update_data(table_name, account=account)