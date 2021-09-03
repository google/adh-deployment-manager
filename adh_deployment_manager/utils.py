import logging
import os
from typing import Dict, Any, List, Optional, Union, NamedTuple
import googleapiclient.discovery  # type: ignore
from googleapiclient.errors import HttpError  # type: ignore
import time
import datetime


def format_date(query_object, base, date_format="%Y-%m-%d"):
    return datetime.datetime.strptime(query_object.get(base), date_format)


def get_date(date_string: str) -> Dict[str, int]:
    """ Formats date in ADH-frienly format.

    Args:
      date_string: date in YYYY-MM-DD format

    Returns:
      Dictionary with year, month, date based on date_string
    """
    return {
        "year": int(date_string.split('-')[0]),
        "month": int(date_string.split('-')[1]),
        "day": int(date_string.split('-')[2])
    }


def execute_adh_api_call_with_retry(
        adh_operation_object: googleapiclient.http.HttpRequest,
        max_retries: int = 10,
        success: Optional[str] = None,
        retries: int = 0) -> Dict[str, Any]:

    while success is None and retries <= max_retries:
        try:
            retries += 1
            operation_response = adh_operation_object.execute()
            success = operation_response
            # if success.get("name"):
            #     logging.info(f'job launched: {success.get("name")}')
        except HttpError as e:
            logging.warning(e._get_reason())
            logging.warning("retrying query")
            time.sleep(10)
    return operation_response


def get_file_content(relative_path: str, working_directory: str = None) -> str:
    """ Reads content of local file and return it as text."""
    if not working_directory:
        working_directory = os.path.dirname(__file__)
    else:
        from pathlib import Path
        working_directory = Path(working_directory)
    with open(os.path.join(working_directory, relative_path),
              "r") as sql_query:
        query_txt = sql_query.read()
        return query_txt.strip()
