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
