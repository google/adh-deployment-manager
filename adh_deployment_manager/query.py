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

import adh_deployment_manager.utils as utils
from googleapiclient.errors import HttpError
import os


class FilteredRowSummary:
    @staticmethod
    def define_filtered_row_summary(parameters):
        par_keys = []
        par_values = []
        for key, values in parameters.items():
            par_keys.append(key)
            parameter_values = values.get("value")
            par_values.append({
                "type": values.get("type"),
                "value": {
                    "value": values.get("value") or ""
                }
            })
        parameters_list = dict(zip(par_keys, par_values))
        return {"columns": parameters_list}


class Parameters:
    @staticmethod
    def define_query_parameters(parameters):
        """Generates parameters in ADH-friendly format.

        Args:
            parameters: List of tuples, each tuple contains parameter
            name, parameter type  ("STRING", "INT64", etc.) and
           whether parameter should be created as an array

        Returns:
            dictionary
        """
        par_keys: List[str] = []
        par_types_full: List[Dict[str, Dict[str, Any]]] = []

        for key, values in parameters.items():
            par_keys.append(key.upper())
            parameter_values = values.get("values")
            if isinstance(parameter_values, list):
                par_types_full.append(
                    {"type": {
                        "arrayType": {
                            "type": values.get("type")
                        }
                    }})
            else:
                par_types_full.append({"type": {"type": values.get("type")}})
        parameters_list = dict(zip(par_keys, par_types_full))
        return parameters_list

    @staticmethod
    def prepare_parameters(parameters, **kwargs):
        """ Generates parameters for execution

        Args:
          parameters: adh parameters
          **kwargs: arbitraty keyword arguments, used to add
          additional parameters at runtime, i.e. parameter="value"

        Returns:
            list of dictionaries in a format prepared for ADH
        """
        par_keys: List[str] = []
        par_values_full: ParameterValues = []
        combined_parameters = {**parameters, **kwargs}
        for key, values in combined_parameters.items():
            if isinstance(values, dict):
                if values.get("values"):
                    parameter_values = values.get("values")
                else:
                    raise ValueError(
                        f"parameter {key} does not have valid values")
                if isinstance(parameter_values, list):
                    par_values_full.append({
                        "arrayValue": {
                            "values": [{
                                'value': f'{value}'
                            } for value in parameter_values]
                        }
                    })
                else:
                    par_values_full.append({"value": f"{parameter_values}"})
            else:
                par_values_full.append({"value": f"{values}"})
            par_keys.append(key.upper())
        parameters_list = dict(zip(par_keys, par_values_full))
        return parameters_list


class AdhQuery:
    def __init__(self,
                 title,
                 text=None,
                 parameters=None,
                 filtered_row_summary=None):
        self.title = title
        self.text = text
        self.parameters = parameters
        self.filtered_row_summary = filtered_row_summary

    def _prepare_query_body(self):
        query_body = {
            "title": self.title,
            "queryText": self.text,
        }
        return query_body

    def format_for_deployment(self):
        query_body = self._prepare_query_body()
        if self.parameters:
            query_body["parameterTypes"] = \
                Parameters.define_query_parameters(self.parameters)
        if self.filtered_row_summary:
            query_body["mergeSpec"] = \
                FilteredRowSummary.define_filtered_row_summary(self.filtered_row_summary)
        return query_body


#TODO: build analysis query static method


class AnalysisQuery(AdhQuery):
    def __init__(self,
                 adh_service,
                 customer_id,
                 ads_data_from=None,
                 query=None,
                 title=None,
                 text='',
                 parameters=None,
                 filtered_row_summary=None,
                 parameterTypes=None,
                 mergeSpec=None):
        if not query and not title:
            raise ValueError(
                "Either AdhQuery object or query title must be provided!")
        self.title = query.title if query else title
        self.text = query.text if query else text
        self.parameters = query.parameters if query else parameters
        self.filtered_row_summary = query.filtered_row_summary if query else filtered_row_summary
        self.parameterTypes = parameterTypes
        self.mergeSpec = mergeSpec
        self.queryState = None
        self.customer_id = f"customers/{customer_id:>09}"
        self.ads_data_from = f"{ads_data_from:>09}" if ads_data_from else f"{customer_id:>09}"
        self.name = None
        self.adh_service = adh_service
        self.query_body_create = None
        self.is_valid_query = None
        self.is_copied = None

    def copy_from(self, copy_from):
        self.customer_id = self.customer_id if self.customer_id else copy_from.title
        self.title = self.title if self.title else copy_from.title
        self.text = self.text if self.text else copy_from.text
        self.parameterTypes = self.parameterTypes if self.parameterTypes else copy_from.parameterTypes
        self.mergeSpec = self.mergeSpec if self.mergeSpec else copy_from.mergeSpec
        self.copy_from = True

    def get(self):
        if self.name:
            filter = f'name="{self.name}"'
        else:
            filter = f'title="{self.title}"'
        op = (self.adh_service.customers().analysisQueries().list(
            parent=self.customer_id, filter=filter))
        query_result = utils.execute_adh_api_call_with_retry(op)
        if query_result:
            query = query_result["queries"][0]
            self.name = query.get("name")
            self.text = query.get("queryText")
            self.parameterTypes = query.get("parameterTypes")
            self.mergeSpec = query.get("mergeSpec")
            self.queryState = query.get("queryState")
        return query_result

    def _create(self):
        if self.is_copied:
            query_body_create = {
                "title": self.title,
                "queryText": self.text,
            }
            if self.copy_from.parameterTypes:
                query_body_create["parameterTypes"] = self.parameterTypes
            if self.copy_from.mergeSpec:
                query_body_create["mergeSpec"] = self.parameterTypes
        else:
            self.query_body_create = super().format_for_deployment()
        return (self.adh_service.customers().analysisQueries().create(
            parent=self.customer_id, body=self.query_body_create))

    def _get_query_body(self):
        return {
            "query": {
                "name": self.name,
                "title": self.title,
                "queryText": self.text,
                "parameterTypes": self.parameterTypes
            }
        }

    def deploy(self, copy_from=None):
        return utils.execute_adh_api_call_with_retry(self._create())

    def validate(self):
        self.get()
        try:
            validation_result = (
                self.adh_service.customers().analysisQueries().validate(
                    parent=self.customer_id,
                    body=self._get_query_body()).execute())
            return (True, None)
        except HttpError as e:
            return (False, e)

    def _run(self,
             start_date,
             end_date,
             output_table_name,
             parameters=None,
             **kwargs):
        if not self.is_valid_query:
            self.is_valid_query = self.validate()
        if not self.is_valid_query[0]:
            raise ValueError(
                f"Cannot start invalid query {self.name}! error: {self.is_valid_query[1]}"
            )
        queryExecuteBody: Dict[str, Any] = {
            "spec": {
                "adsDataCustomerId": self.ads_data_from,
                "startDate": utils.get_date(start_date),
                "endDate": utils.get_date(end_date)
            },
            "destTable": output_table_name
        }
        if parameters:
            queryExecuteBody["spec"]["parameterValues"] = \
                Parameters.prepare_parameters(parameters, **kwargs)

        op = (self.adh_service.customers().analysisQueries().start(
            name=self.name, body=queryExecuteBody))
        return op

    def run(op):
        run_query = utils.execute_adh_api_call_with_retry(op)
        return run_query

    def update(self,
               title=None,
               text=None,
               parameters=None,
               filtered_row_summary=None):
        if not self.name:
            self.get()
        query_body = {
            "title":
            title if title else self.title,
            "queryText":
            text if text else self.text,
            "parameterTypes":
            Parameters.define_query_parameters(parameters)
            if parameters else self.parameterTypes,
            "mergeSpec":
            FilteredRowSummary.define_filtered_row_summary(
                filtered_row_summary)
            if filtered_row_summary else self.mergeSpec
        }

        op = (self.adh_service.customers().analysisQueries().patch(
            name=self.name, body=query_body))
        updated_query = utils.execute_adh_api_call_with_retry(op)
        if updated_query:
            self.title = updated_query.get("title")
            self.text = updated_query.get("queryText")
            self.parameterTypes = updated_query.get("parameterTypes")
            self.mergeSpec = updated_query.get("mergeSpec")
        return updated_query

    # TODO: add relative and absolute files
    # TODO: fix saving into file folder (adh_deployment_manager)
    def dump(self, location, file_name=None, extension=".sql"):
        if not self.text:
            self.get()
        working_directory = os.path.dirname(__file__)
        dump_location = os.path.join(working_directory, location)
        if not os.path.exists(dump_location):
            os.makedirs(dump_location)
        if not file_name:
            file_name = self.title
        with open(os.path.join(dump_location, f"{file_name}{extension}"),
                  "w") as f:
            f.write(self.text)
