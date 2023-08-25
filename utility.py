import json


def to_list(value):
    """
    Converts a string to a list.
    Returns a list if it's already a list.
    """
    if isinstance(value, str):
        value = [value]
    return value


def flatten_json_list_values(json_obj, exclude_keys=[]):
    """
    Flattens json keys with listed values
    """
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


def reposition_columns(df, col_positions={}):
    """
    Repositions column names of a pandas df.
    """
    cols = [col for col in df.columns if col not in col_positions]

    # Organize the column order
    for col in col_positions:
        cols.insert(col_positions[col], col)

    df = df[cols]
    return df