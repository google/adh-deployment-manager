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

from googleapiclient.discovery import build  # type: ignore
import adh_deployment_manager.utils as utils
from adh_deployment_manager.job import _is_adh_job_running

_ADH_DISCOVERY_SERVICE_URL = "https://adsdatahub.googleapis.com/$discovery/rest?version=v1"


class AdhService:
    def __init__(self,
                 credentials,
                 developer_key,
                 serviceName="AdsDataHub",
                 version="v1",
                 discoveryServiceUrl=_ADH_DISCOVERY_SERVICE_URL):
        self.adh_service = build(serviceName=serviceName,
                                 version=version,
                                 credentials=credentials,
                                 developerKey=developer_key,
                                 discoveryServiceUrl=discoveryServiceUrl)

    def get_running_jobs(self):
        """ Get all running jobs.

        Args:
          adh_service: ADH service object

        Returns:
          List of running jobs as dict {"name": "startTime"}
        """

        op = self.adh_service.operations().list(name='operations')
        adh_operations = utils.execute_adh_api_call_with_retry(op)
        running_jobs = {}
        for operation in adh_operations["operations"]:
            if _is_adh_job_running(operation["metadata"]):
                running_jobs[
                    operation["name"]] = operation["metadata"]["startTime"]
        return running_jobs
