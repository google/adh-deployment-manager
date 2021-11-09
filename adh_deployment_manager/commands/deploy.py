from .abs_command import AbsCommand
import logging

class Deployer(AbsCommand):
    def __init__(self,
                 deployment):
       self.deployment = deployment

    def execute(self, update=False, **kwargs):
        deployed_queries = []
        # iterate over each query in config
        queries = self.deployment._get_queries(is_buildable=True)
        for adh_query, analysis_query in queries:
            query = adh_query.title
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
                self.deployment.queries[query] = deployed_query.get("name")
            # if query is in the project already do nothing
            else:
                logging.warning(
                    f"query {query} cannot be deployed because it exists.")
        return deployed_queries


