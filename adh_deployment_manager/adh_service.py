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
