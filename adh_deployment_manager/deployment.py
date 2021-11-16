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

# TODO: implement overwrites (adh_project, bq_project & dataset)
import logging
import abc
from typing import NamedTuple
from adh_deployment_manager.adh_service import AdhService
from adh_deployment_manager.job import Job, wait_for_query_success
from adh_deployment_manager.config import Config
from adh_deployment_manager.query import AdhQuery, AnalysisQuery
from adh_deployment_manager.utils import format_date, get_file_content, execute_adh_api_call_with_retry
from pandas import date_range  # type: ignore
import datetime

class AdhAnalysisQuery(NamedTuple):
    adh_query: AdhQuery
    analysis_query: AnalysisQuery

class Deployment:
    def __init__(self,
                 config,
                 developer_key,
                 credentials,
                 queries_folder="sql",
                 query_file_extention=".sql"):
        self.config = Config(config)
        self.adh_service = AdhService(credentials, developer_key)
        self.queries_folder = queries_folder
        self.query_file_extention = query_file_extention
        self.queries = {}

    def get_adh_service(self):
        return self.adh_service.adh_service

    def print_config(self):
        try:
            print(self.config)
        except TypeError:
            pass

    def __str__(self):
        queries = self.config.queries.keys()
        deployment_message = f"""
Total {len(queries)} queries will be deployed for customer{"s"if len(queries) > 1 else ''}:
    {', '.join(str(customer_id) for customer_id in self.config.customer_id)}
List of queries:
    {', '.join(self.config.queries.keys())}.
        """
        return deployment_message

    def _get_queries(self, is_buildable=False):
        for query in self.config.queries:
            query_for_run = self.config.queries[query]
            if is_buildable:
                adh_query = AdhQuery(
                    query,
                    get_file_content(
                        f"{self.queries_folder}/{query}{self.query_file_extention}"
                    ),
                    query_for_run.get("parameters"),
                    query_for_run.get("filtered_row_summary"))
            else:
                adh_query = AdhQuery(query)
            for customer_id, ads_data_from in zip(self.config.customer_id,
                                                  self.config.ads_data_from):
                # create AnalysisQuery object for deployment and / or run
                analysis_query = AnalysisQuery(
                    adh_service=self.adh_service.adh_service,
                    customer_id=customer_id,
                    ads_data_from=ads_data_from,
                    query=adh_query)
                yield AdhAnalysisQuery(
                    adh_query=adh_query,
                    analysis_query=analysis_query)
