import logging.config
import json
import os

def setup_logging(script_path):
    """
    Set up logging based on the provided script's path.

    Args:
    - script_path (str): The full path to the calling script.
    """

    # Deduce the script's name and adjust the log filename accordingly
    script_name = os.path.splitext(os.path.basename(script_path))[0]
    log_filename = f"logging/{script_name}.log"

    # Load the JSON logging config and modify the log filename
    with open('logging_config.json', 'r') as f:
        config = json.load(f)
        config['handlers']['file']['filename'] = log_filename

    # Apply the logging configuration
    logging.config.dictConfig(config)