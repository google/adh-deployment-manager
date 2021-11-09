import logging
from .abs_command import AbsCommand

class Updater(AbsCommand):
    def __init__(self,
                 deployment):
       self.deployment = deployment

    def execute(self, **kwargs):
        updated_queries = []
        queries = self.deployment._get_queries(is_buildable=True)
        for adh_query, analysis_query in queries:
            query = adh_query.title
            if analysis_query.get():
                logging.info(f"updating query: {query}...")
                updated_query = analysis_query.update(
                    title=adh_query.title,
                    text=adh_query.text,
                    parameters=adh_query.parameters)
                updated_queries.append(updated_query)
                self.deployment.queries[query] = updated_query.get("name")
            else:
                logging.warning(
                    f"query {query} cannot be found, would you like to deploy?"
                )
        return updated_queries
