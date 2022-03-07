"""
Database Fetcher module
"""

import json
from typing import Any, Dict

import requests


def fetch_metadata_json(source_db_location: str, data_id: str) -> Dict[str, Any]:
    """
    Fetch metadata from json

    Parameters
    ----------
    source_db_location : str
        Source json file location
    data_id : str
        data identifier

    Returns
    -------
    Dict[str, Any]
        metadata of the choosen data
    """
    # REFACTOR: add singleton design pattern to make no repeated I/O
    # I/O is expensive
    with open(source_db_location, "r+", encoding="utf-8") as file:
        json_dict: Dict[str, Dict[str, Any]] = json.load(file)
    return json_dict[data_id]


def fetch_metadata_url_json(source_db_location: str, data_id: str) -> Dict[str, Any]:
    """
    Fetch metadata URL that contains json file

    Parameters
    ----------
    source_db_location : str
        Source database location
    data_id : str
        Database id

    Returns
    -------
    Dict[str, Any]
        Return the provided data
    """
    request_result = requests.get(source_db_location)
    return request_result.json()[data_id]  # type: ignore


def fetch_data_meta(
    source_db_type: str, source_db_location: str, data_id: str
) -> Dict[str, Any]:
    """
    Fetch metadata from some file

    Parameters
    ----------
    source_db_type : str
        Source type 'json_file' or 'url'
    source_db_location : str
        Source location database
    data_id : str
        data identifier of the model

    Returns
    -------
    Dict[str, Any]
        metadata of the data
    """
    if source_db_type == "json_file":
        returned_data = fetch_metadata_json(
            source_db_location=source_db_location, data_id=data_id
        )
    elif source_db_type == "url":
        returned_data = fetch_metadata_url_json(
            source_db_location=source_db_location, data_id=data_id
        )
    else:
        raise NotImplementedError(f"{source_db_type} is not implemented!")
    return returned_data
