from .abs_command import AbsCommand
from .deploy import Deployer
from adh_deployment_manager.utils import format_date, get_file_content, execute_adh_api_call_with_retry
from adh_deployment_manager.job import wait_for_query_success
import logging

class Runner(AbsCommand):
    def __init__(self,
                 deployment):
        self.deployment = deployment
        self.adh_service = deployment.adh_service.adh_service
        self.config = deployment.config

    def launch_job(self, job, wait):
        launched_job = execute_adh_api_call_with_retry(job)
        if launched_job:
            logging.info("job successfully launched!")
        if wait:
            wait_for_query_success(self.adh_service,
                                   launched_job.get("name"))
        return launched_job.get("name")

    def execute(self, deploy=False, update=False, **kwargs):
        from collections import deque
        # TODO: evaluate whether we need deque
        job_queue = deque()
        # TODO: return Job instance instead of operation names
        launched_job_queue = deque()


        if not self.config.bq_project or not self.config.bq_dataset:
            logging.error("BQ project and/or dataset weren't provided")
            raise ValueError(
                "BQ project and dataset are required to run the queries!")
        if deploy:
            deployer = Deployer(deployment)
            deployer.execute(update=update)
        # iterate over queries in config
        queries = self.deployment._get_queries()
        for adh_query, analysis_query in queries:
            query = adh_query.title
            # check if deployment already contains name of the query
            if query not in self.config.queries.keys():
                # deploy query if it's not in the project
                if not analysis_query.get():
                    # check if `deploy` option is specified
                    logging.error(
                        f"{query} is not found for customer {customer_id}")
                    continue
            query_for_run = self.config.queries[query]
            logging.info(f"setting up query for run: {query}...")
            output_table_suffix = query_for_run.get("output_table_suffix")
            table_name = f"{query}{output_table_suffix}" if output_table_suffix else query
            if not query_for_run.get("batch_mode"):
                #TODO: if query failed due to 100,000 user sets error, switch to batch mode
                job = analysis_query._run(
                    query_for_run.get("start_date"),
                    query_for_run.get("end_date"),
                    f"{self.config.bq_project}.{self.config.bq_dataset}.{table_name}",
                    query_for_run.get("parameters"), **kwargs)
                launched_job = self.launch_job(job=job,
                                          wait=query_for_run.get("wait"))
                job_queue.append({
                    "job_obj": job,
                    "wait_status": query_for_run.get("wait"),
                    "query_identifier": f"{query}"
                })
            else:
                min_date = format_date(query_for_run, "start_date")
                max_date = format_date(query_for_run, "end_date")
                dates_array = date_range(min_date, max_date, freq='d')
                for i, date in enumerate(dates_array):
                    fetching_date = date.strftime("%Y-%m-%d")
                    job = analysis_query._run(
                        fetching_date, fetching_date,
                        f"{self.config.bq_project}.{self.config.bq_dataset}.{output_table_suffix}_{date.strftime('%Y%m%d')}",
                        query_for_run.get("parameters"), **kwargs)
                    if i == (len(dates_array) - 1):
                        wait = query_for_run.get("wait")
                    else:
                        wait = False
                    launched_job = self.launch_job(job, wait=wait)
                    job_queue.append({
                        "job_obj":
                        job,
                        "wait_status":
                        wait,
                        "query_identifier":
                        f"{query}_{date.strftime('%Y%m%d')}"
                    })
            launched_job_queue.append(launched_job)
        return {"jobs": job_queue, "launched_jobs": launched_job_queue}
