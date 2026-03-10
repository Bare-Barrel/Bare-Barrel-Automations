import pandas_gbq
from google.auth import default as google_auth_default
import logging
import logger_setup


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)


def load_to_bigquery(df, table_id, project_id, load_type):
    """
    Load dataframe to a BigQuery table.
    """
    try:
        logger.info("Loading DataFrame to BigQuery...")
        credentials, project = google_auth_default()
        pandas_gbq.to_gbq(
            df, 
            destination_table = table_id, 
            project_id = project_id, 
            credentials = credentials,    # automatically loaded from env
            if_exists = load_type
        )
        logger.info("Data loaded successfully.")
    except Exception as e:
        logger.info("Error loading data to BigQuery:", e)