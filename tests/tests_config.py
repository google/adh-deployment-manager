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

from adh_deployment_manager.config import Config
from adh_deployment_manager.query import Parameters
import adh_deployment_manager.utils as utils

# define sample config used for running test against
_SAMPLE_CONFIG_PATH = "sample_config.yml"
_CONFIG = {
    "queries_setup": [
        {
            "queries": ["sample_query_1_1", "sample_query_1_2"],
            "parameters": {
                "parameter_1": {
                    "type": "STRING",
                    "values": "1"
                }
            },
            "wait": "each",
        },
        {
            "queries": ["sample_query_2_1", "sample_query_2_2"],
            "parameters": {
                "parameter_atomic": {
                    "type": "STRING",
                    "values": "1"
                },
                "parameter_array": {
                    "type": "STRING",
                    "values": ["1", "2"]
                }
            },
            "wait": "block",
            "execution_mode": "batch"
        },
        {
            "queries": ["sample_query_3"],
            "execution_mode": "normal",
            "replace": {
                "key": "value"
            }
        },
    ]
}


# Define fixtures to be used by pytest
# Define common setup that contains list of queries
@pytest.fixture
def setup():
    config = Config(_SAMPLE_CONFIG_PATH, os.path.dirname(__file__))
    config.config = _CONFIG
    return config.extract_queries_setup()


# define setup that does not contain valid key 'queries'
@pytest.fixture
def broken_setup():
    return {
        "queries_setup": [
            {
                "query": ["sample_query_1_1", "sample_query_1_2"],
                "parameters": {
                    "parameter_1": {
                        "type": "STRING",
                        "values": "1"
                    }
                },
                "wait": "each",
            },
        ]
    }


### TESTS
# verify that reading config returns required set of elements to run script
def test_get_config(setup):
    config = Config(_SAMPLE_CONFIG_PATH, os.path.dirname(__file__))
    assert set(["developer_key", "bq_project", "bq_dataset",
                "queries_setup"]).issubset(config.get_config().keys())


# extract_queries_setup returns correct 'wait' modes
@pytest.mark.parametrize(
    "expected,query",
    [
        (True, "sample_query_1_1"),  # wait on each query in the block
        (True, "sample_query_1_2"),  # wait on each query in the block
        (False, "sample_query_2_1"),  # don't wait for first query in the block
        (True, "sample_query_2_2"),  # wait on each query in the block
        (False, "sample_query_3")  # don't wait for query
    ])
def test_extract_queries_setup_wait_mode(setup, expected, query):
    assert expected == setup[query]["wait"]


# extract_queries_setup returns None if replacements aren't specified
def test_extract_queries_setup_empty_replacements(setup):
    assert setup["sample_query_1_1"].get("replacements") is None


# extract_queries_setup returns not empty result  if replacements are specified
def test_extract_queries_setup_not_empty_replacements(setup):
    assert setup["sample_query_3"].get("replacements") is not None


# extract_queries_setup returns dict  if replacements are specified
def test_extract_queries_setup_replacements_type(setup):
    assert isinstance(setup["sample_query_3"].get("replacements"), dict)


# extract_queries_setup return correct 'batch' modes
@pytest.mark.parametrize(
    "expected,query",
    [
        (False,
         "sample_query_1_1"),  # batch mode not specified, False by default
        (True, "sample_query_2_1")  # batch mode specified as 'batch' -> True
    ])
def test_extract_queries_setup_execution_mode(setup, expected, query):
    assert expected == setup[query]["batch_mode"]


# broken setup - raises KeyError
@pytest.mark.skip(reason="Implement KeyError expection in a method")
def test_extract_queries_setup_broken_setup(broken_setup):
    config = Config(_SAMPLE_CONFIG_PATH, os.path.dirname(__file__))
    config.queries = broken_setup
    with pytest.raises(KeyError):
        queries_setup = config.extract_queries_setup()


# _define_query_parameters returns correct values when parsing atomic parameters
def test_define_query_parameters_single():
    parameters = {"parameter_atomic": {"type": "STRING", "values": "1"}}
    defined_parameters = Parameters.define_query_parameters(parameters)
    assert defined_parameters.get("PARAMETER_ATOMIC") == {
        "type": {
            "type": "STRING"
        }
    }
