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

import pytest
import os

import adh_deployment_manager.utils as utils

# date constant
_DATE = "1970-01-01"


# _get_date returns a dict
def test_get_date_type():
    sample_date = utils.get_date(_DATE)
    assert isinstance(sample_date, dict)


# _get_date returns a properly formatted dict
def test_get_date_value():
    sample_date = utils.get_date(_DATE)
    assert sample_date == {"year": 1970, "month": 1, "day": 1}


# _get_file_content returns valid content
def test_get_file_content():
    text = utils.get_file_content("sample_query.sql",
                                  os.path.dirname(__file__))
    assert text == "SELECT test_field FROM test_table"
