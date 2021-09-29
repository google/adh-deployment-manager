# TODO: implement overwrites (adh_project, bq_project & dataset)
import logging
from adh_deployment_manager.adh_service import AdhService
from adh_deployment_manager.job import Job, wait_for_query_success
from adh_deployment_manager.config import Config
from adh_deployment_manager.query import AdhQuery, AnalysisQuery
from adh_deployment_manager.utils import format_date, get_file_content, execute_adh_api_call_with_retry
from pandas import date_range  # type: ignore
import datetime


class Deployment:
    def __init__(self,
                 config,
                 credentials,
                 queries_folder="sql",
                 query_file_extention=".sql"):
        self.config = Config(config)
        self.adh_service = AdhService(credentials, self.config.developer_key)
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

    #TODO: generate config based on template (we have a minimal template with query names, method goes to adh project, fetches parameters and updates config for each query)
    def populate(self, location):
        for query in self.config.queries:
            query_for_run = self.config.queries[query]
            # create query out of text files provided
            adh_query = AdhQuery(query)
            for customer_id in self.config.customer_id:
                # initialize adh project
                analysis_query = AnalysisQuery(
                    adh_service=self.adh_service.adh_service,
                    customer_id=customer_id,
                    query=adh_query)
                query_output = analysis_query.get()
                logging.debug(query_output)
                if query_output:
                    analysis_query.dump(location)
                    query_result = query_output.get("queries")[0]
                    if query_result.get("parameterTypes"):
                        for param, types in query_result.get(
                                "parameterTypes").items():
                            print(f'{param}: {types.get("type").get("type")}')
                    if query_result.get("mergeSpec"):
                        for field, values in query_result.get("mergeSpec").get(
                                "columns").items():
                            print(
                                f'{field}: {values.get("type")}: {values.get("value").get("value")}'
                            )
                break

    def deploy(self, update=False):
        deployed_queries = []
        # iterate over each query in config
        for query in self.config.queries:
            # select query for deployment
            query_for_run = self.config.queries[query]
            # create query out of text files provided
            adh_query = AdhQuery(
                query,
                get_file_content(
                    f"{self.queries_folder}/{query}{self.query_file_extention}"
                ), query_for_run.get("parameters"),
                query_for_run.get("filtered_row_summary"))
            # iterate over each customer_id in the config
            # TODO: parallelize deployment on different threads
            for customer_id in self.config.customer_id:
                # initialize adh project
                # initialize analysis query
                analysis_query = AnalysisQuery(
                    adh_service=self.adh_service.adh_service,
                    customer_id=customer_id,
                    query=adh_query)
                # check if query with provided title is found in the project
                if not analysis_query.get():
                    logging.info(f"deploying query: {query}...")
                    # deploy query
                    deployed_query = analysis_query.deploy()
                    # add query to the list of deployed queries
                    deployed_queries.append(deployed_query)
                    self.queries[query] = deployed_query.get("name")
                # if update flag is specified update existing query
                elif update:
                    logging.info(f"updating query: {query}...")
                    deployed_query = analysis_query.update(
                        title=adh_query.title,
                        text=adh_query.text,
                        parameters=adh_query.parameters,
                        filtered_row_summary=adh_query.filtered_row_summary)
                    deployed_queries.append(deployed_query)
                    self.queries[query] = deployed_query.get("name")
                # if query is in the project already do nothing
                else:
                    logging.warning(
                        f"query {query} cannot be deployed because it exists.")
        return deployed_queries

    def update_only(self):
        updated_queries = []
        for query in self.config.queries:
            query_for_run = self.config.queries[query]
            adh_query = AdhQuery(
                query,
                get_file_content(
                    f"{self.queries_folder}/{query}{self.query_file_extention}"
                ), query_for_run.get("parameters"),
                query_for_run.get("filtered_row_summary"))
            # TODO: parallelize deployment on different threads
            for customer_id in self.config.customer_id:
                analysis_query = AnalysisQuery(
                    adh_service=self.adh_service.adh_service,
                    customer_id=customer_id,
                    query=adh_query)
                if analysis_query.get():
                    logging.info(f"updating query: {query}...")
                    updated_query = analysis_query.update(
                        title=adh_query.title,
                        text=adh_query.text,
                        parameters=adh_query.parameters)
                    updated_queries.append(updated_query)
                    self.queries[query] = updated_query.get("name")
                else:
                    logging.warning(
                        f"query {query} cannot be found, would you like to deploy?"
                    )
        return updated_queries

    def deploy_and_run(self):
        d = self.deploy()
        self.run()

    def run(self, deploy=False, update=False, **kwargs):
        from collections import deque
        # TODO: evaluate whether we need deque
        job_queue = deque()
        # TODO: return Job instance instead of operation names
        launched_job_queue = deque()

        def launch_job(job, wait):
            launched_job = execute_adh_api_call_with_retry(job)
            if launched_job:
                logging.info("job successfully launched!")
            if wait:
                wait_for_query_success(self.adh_service.adh_service,
                                       launched_job.get("name"))
            return launched_job.get("name")

        if not self.config.bq_project or not self.config.bq_dataset:
            logging.error("BQ project and/or dataset weren't provided")
            raise ValueError(
                "BQ project and dataset are required to run the queries!")
        if deploy:
            self.deploy(update=update)
        # iterate over queries in config
        for query in self.config.queries:
            # construct AdhQuery object with query title
            adh_query = AdhQuery(query)
            # iterate over customer_ids in config
            # TODO: parallelize deployment on different threads
            for customer_id in self.config.customer_id:
                # create AnalysisQuery object for deployment and / or run
                analysis_query = AnalysisQuery(
                    adh_service=self.adh_service.adh_service,
                    customer_id=customer_id,
                    query=adh_query)
                # check if deployment already contains name of the query
                if query not in self.queries.keys():
                    # deploy query if not if it's not in the project
                    if not analysis_query.get():
                        # check if `deploy` option is specified
                        logging.error(
                            f"{query} is not found for customer {customer_id}")
                        continue
                query_for_run = self.config.queries[query]
                logging.info(f"setting up query for run: {query}...")
                if not query_for_run.get("batch_mode"):
                    #TODO: if query failed due to 100,000 user sets error, switch to batch mode
                    job = analysis_query._run(
                        query_for_run.get("start_date"),
                        query_for_run.get("end_date"),
                        f"{self.config.bq_project}.{self.config.bq_dataset}.{query}",
                        query_for_run.get("parameters"), **kwargs)
                    launched_job =  launch_job(job, wait = query_for_run.get("wait"))
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
                            f"{self.config.bq_project}.{self.config.bq_dataset}.{query}_{date.strftime('%Y%m%d')}",
                            query_for_run.get("parameters"), **kwargs)
                        if i == (len(dates_array) - 1):
                            wait = query_for_run.get("wait")
                        else:
                            wait = False
                        launched_job = launch_job(job, wait = wait)
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
