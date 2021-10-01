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

from adh_deployment_manager.query import Parameters

# define sample config used for running test against
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

# date constant
_DATE = "1970-01-01"


# Define fixtures to be used by pytest
# Define common setup that contains list of queries
@pytest.fixture
def setup():
    config = Config("tests/sample_config.yml")
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


# define invalid parameters that are empty
@pytest.fixture
def broken_parameters():
    return {"parameter_empty_values": {"type": "STRING", "values": None}}


### TESTS
# _define_query_parameters returns correct values when parsing atomic parameters
def test_define_query_parameters_single():
    parameters = {"parameter_atomic": {"type": "STRING", "values": "1"}}
    defined_parameters = Parameters.define_query_parameters(parameters)
    assert defined_parameters.get("PARAMETER_ATOMIC") == {
        "type": {
            "type": "STRING"
        }
    }


# _define_query_parameters returns correct values when parsing array parameters
def test_define_query_parameters_array():
    parameters = {"parameter_array": {"type": "STRING", "values": ["1", "2"]}}
    defined_parameters = Parameters.define_query_parameters(parameters)
    assert defined_parameters.get("PARAMETER_ARRAY") == {
        "type": {
            "arrayType": {
                "type": "STRING"
            }
        }
    }


# _prepare_parameters returns correct values when parsing atomic parameters
def test_prepare_parameters_single(prepared_parameters):
    prepared_parameters = Parameters.prepare_parameters(parameters)
    assert prepared_parameters.get("PARAMETER_ATOMIC") == {"value": "1"}


# _prepare_parameters returns correct values when parsing atomic parameters
def test_prepare_parameters_single():
    parameters = {"parameter_atomic": {"type": "STRING", "values": "1"}}
    prepared_parameters = Parameters.prepare_parameters(parameters)
    assert prepared_parameters.get("PARAMETER_ATOMIC") == {"value": "1"}


#  _prepare_parameters returns correct values when parsing array parameters
def test_prepare_parameters_array():
    parameters = {"parameter_array": {"type": "STRING", "values": ["1", "2"]}}
    prepared_parameters = Parameters.prepare_parameters(parameters)
    assert prepared_parameters.get("PARAMETER_ARRAY") == {
        "arrayValue": {
            "values": [{
                "value": "1"
            }, {
                "value": "2"
            }]
        }
    }


# _prepare_parameters raises ValueError when parameter contains empty values
def test_prepare_parameters_broken_parameters(broken_parameters):
    with pytest.raises(ValueError):
        prepared_parameters = \
            Parameters.prepare_parameters(broken_parameters)
