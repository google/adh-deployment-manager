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

import time
import logging
import adh_deployment_manager.utils as utils


def _is_adh_job_running(job_object_metadata):
    return job_object_metadata["endTime"] == "1970-01-01T00:00:00Z"


def check_operation_status(adh_service, job_id):
    """ Check status of a running operation

    Args:
      adh_service: ADH service object
      job_id: adh job_id in a format operations/912udkjfakdsjfw0

    Returns:
      Status of the job, one of Running, Error, Success
    """
    op = adh_service.operations().get(name=job_id)
    operation_status = utils.execute_adh_api_call_with_retry(op)
    if _is_adh_job_running(operation_status["metadata"]):
        status = "Running"
    elif "error" in operation_status.keys():
        status = "Error"
    else:
        status = "Success"
    return {"status": status, "errors": operation_status.get("error")}


def wait_for_query_success(adh_service, job_id, delay: int = 30) -> None:
    # give ADH additional time to register a job
    time.sleep(10)
    # poll query operation status
    operation_status = check_operation_status(adh_service, job_id)
    logging.info(f'current job status is {operation_status.get("status")}')
    while operation_status.get("status") == "Running":
        time.sleep(delay)
        operation_status = check_operation_status(adh_service, job_id)


class Job():
    def __init__(self, name, adh_service):
        self.name = name
        self.status = None
        self.adh_service = adh_service.adh_service

    def get_status(self):
        job_status = check_operation_status(self.adh_service, self.name)
        self.status = job_status
        return job_status

    def stop(self):
        op = self.adh_service.operations().cancel(name=self.name)
        result = utils.execute_adh_api_call_with_retry(op)
        return result
