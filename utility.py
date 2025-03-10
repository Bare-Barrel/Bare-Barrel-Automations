import json
import datetime as dt
import pandas as pd
import subprocess
import logging
import logger_setup


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

def to_list(value):
    """
    Converts a string to a list.
    Returns a list if it's already a list.
    """
    if not isinstance(value, list):
        value = [value]
    return value


def is_json_nested(json_obj):
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if isinstance(value, (dict, list)):
                return True
            if is_json_nested(value):
                return True
    elif isinstance(json_obj, list):
        for item in json_obj:
            if isinstance(item, (dict, list)):
                return True
            if is_json_nested(item):
                return True
    return False


def flatten_json_list_values(json_obj, exclude_keys=[]):
    """
    Flattens json keys with listed values
    """
    if isinstance(json_obj, list):
        return json_obj
    
    flattened = {}

    for key, value in json_obj.items():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            aggregated_dict = {}
            for item in value:
                if item in exclude_keys:
                    continue
                for k, v in item.items():
                    aggregated_dict[f"{key}_{k}"] = v
            flattened.update(aggregated_dict)
        else:
            flattened[key] = value
            
    return flattened


def payload_to_dataframe(response, get_key):
    """
    Transforms a response's payload to a dataframe
    """
    payload = response.payload
    if get_key:
        payload = payload.get(get_key)

    if not payload:
        return pd.DataFrame()

    flattened_json = flatten_json_list_values(payload)

    try:
        return pd.json_normalize(flattened_json, sep='_')
    except ValueError:
        return pd.read_json(flattened_json)


def reposition_columns(df, col_positions={}):
    """
    Repositions column names of a pandas df.

    Args:
        df (pd.DataFrame)
        col_position (dict): {col_name: col_position}
            e.g.: {'date': 0, 'marketplace': 1}
    
    Returns:
        df (pd.DataFrame)
    """
    if df.empty:
        logger.warning("Empty dataframe")
        return df
    
    try:
        cols = [col for col in df.columns if col not in col_positions]

        # Organize the column order
        for col in col_positions:
            cols.insert(col_positions[col], col)

        df = df[cols]

    except KeyError as error:
        logger.error("Failed repositioning dataframe!")
        logger.error(error)

    return df


def to_date(date):
    """
    Transforms str date ('YYYY-MM-DD' | 'DD-MM-YYY') to datetime.date
    """
    if isinstance(date, str):
        if len(date.split('-')[0]) == 4:
            date = dt.datetime.strptime(date, '%Y-%m-%d').date()
        else:
            date = dt.datetime.strptime(date, '%m-%d-%Y').date()
    return date


def get_day_of_week(target_date, desired_day):
    """
    Get the closest specified day of the week from the given date.
    
    Parameters:
    - target_date (str | datetime.date): The reference date
    - desired_day (str): The desired day of the week. Should be one of ["Monday", "Tuesday", ..., "Sunday"]
    
    Returns:
    - datetime.date: The date of the closest desired day of the week
    """
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    if desired_day not in days:
        raise ValueError(f"'{desired_day}' is not a valid day of the week. Choose from {days}")

    target_date = to_date(target_date)
    current_day_index = target_date.weekday()  # Monday is 0, Sunday is 6
    desired_day_index = days.index(desired_day)

    # Calculate the difference in days between current and desired day
    day_difference = desired_day_index - current_day_index
    if day_difference <= 0:  # if desired day is before or same as current day, go to the next week's desired day
        day_difference += 7

    # Return the resulting date
    return target_date + dt.timedelta(days=day_difference)


def sync_with_rclone(source, destination, config_path=None):
    """
    Run an rclone sync command from Python, syncing `source` to `destination`.
    e.g., source="google_drive:FolderA", destination="/home/user/localFolder"
    """
    try:
        # Construct the command as a list of arguments
        cmd = [
            "rclone", 
            "sync", 
            source, 
            destination, 
            "--progress",  # optional: show progress in the terminal
            # Add any other flags you want, e.g. "--dry-run"
        ]

        if config_path:
            cmd.extend(["--config", config_path])

        logger.info(f"Syncing {source} to {destination}")
        
        # Run the command, check=True means it raises an exception if the exit code is non-zero
        subprocess.run(cmd, check=True)
        logger.info("Sync completed successfully!")
    except subprocess.CalledProcessError as e:
        # This exception is raised if rclone exits with a non-zero status
        logger.error(f"Error: rclone sync failed with error code {e.returncode}")
        raise e
    except FileNotFoundError:
        # Raised if the 'rclone' binary isn't found on the system
        logger.error("Error: rclone is not installed or not found in PATH.")
        raise FileNotFoundError
