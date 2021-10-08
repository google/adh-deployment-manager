#
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        query_lines = sql_query.readlines()
        query_txt = "".join(line for line in query_lines
                            if not line.startswith("#"))
        return query_txt.strip()
