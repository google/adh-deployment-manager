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

#TODO: validate config

import logging
import os
import yaml
from collections import OrderedDict
import adh_deployment_manager.utils as utils


class Config:
    def __init__(self, path, working_directory=None):
        self.path = path
        self.working_directory = working_directory
        # TODO: duplicate fields for config
        self.config = self.get_config()
        self.customer_id = self._atomic_to_list(self.config.get("customer_id"))
        self.ads_data_from = self._atomic_to_list(
            self.config.get("ads_data_from")) or self._atomic_to_list(
                self.config.get("customer_id"))
        self.bq_project = self.config.get("bq_project")
        self.bq_dataset = self.config.get("bq_dataset")
        self.queries = self.extract_queries_setup()
        self.start_date = self.convert_date("start_date")
        self.end_date = self.convert_date("end_date")

    def _atomic_to_list(self, obj):
        if isinstance(obj, list):
            return obj
        else:
            return [obj]

    def __str__(self):
        import sys
        # TODO: fix TypeError: __str__ returned non-string (type NoneType)
        return yaml.dump(self.config, sys.stdout, sort_keys=False)

    def convert_date(self, base):
        if self.config.get("date_range_setup"):
            return self._convert_date(
                str(self.config.get("date_range_setup").get(base)))
        else:
            # TODO: provide more meaningful message
            logging.error("Cannot get information on dates")

            return None

    def _convert_date(self, date_string):
        if date_string.find("YYYYMMDD") == -1:
            return date_string
        else:
            from datetime import date, timedelta
            days_ago = date_string.index("-") + 1
            new_date = date.today() - timedelta(
                days=int(date_string[days_ago:]))
            return new_date.strftime("%Y-%m-%d")

    # TODO: make get_config independent of a file (we can specify it as already
    # provided structure (yaml was already loaded)
    def get_config(self):
        """ Read config.yml file and return key elements."""
        if not self.working_directory:
            self.working_directory = os.path.dirname(__file__)
        with open(os.path.join(self.working_directory, self.path),
                  "r") as config:
            cfg = yaml.load(config, Loader=yaml.SafeLoader)
            return cfg

    def extract_queries_setup(self):
        """ Extract queries_setup from config.yml and maps query to parameters."""
        query_names: Query = OrderedDict()
        queries_setup = self.config.get("queries_setup")
        for i, setups in enumerate(queries_setup):
            wait_for_query = False
            start_date = None
            end_date = None
            date_range_block = setups.get("date_range")
            if date_range_block:
                start_date = self._convert_date(
                    str(date_range_block.get("start_date")))
                end_date = self._convert_date(
                    str(date_range_block.get("end_date")))
            end_block = i == (len(queries_setup) - 1)
            try:
                queries = setups["queries"]
                if not queries:
                    continue
                max_queries = len(queries)
                for j, query in enumerate(queries):
                    end_query = j == (max_queries - 1)
                    if setups.get("wait") == "each":
                        wait_for_query = True
                    elif setups.get("wait") == "block":
                        if j == (max_queries - 1):
                            wait_for_query = True
                    query_names[query] = {
                        "start_date":
                        start_date
                        if start_date else self.convert_date("start_date"),
                        "end_date":
                        end_date
                        if end_date else self.convert_date("end_date"),
                        "wait":
                        False if end_block and end_query else wait_for_query,
                        "parameters":
                        setups.get("parameters"),
                        "filtered_row_summary":
                        setups.get("filtered_row_summary"),
                        "batch_mode":
                        setups.get("execution_mode") == "batch",
                        "replacements":
                        setups.get("replace"),
                        "output_table_suffix":
                        setups.get("output_table_suffix")
                    }
            except KeyError:
                raise KeyError("No queries specified in query block!")
        return query_names
