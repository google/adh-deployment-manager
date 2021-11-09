from .abs_command import AbsCommand
import logging

class Populator(AbsCommand):
    def __init__(self,
                 deployment):
        self.deployment = deployment

    def execute(self, location, **kwargs):
        queries = self.deployment._get_queries()
        for adh_query, analysis_query in queries:
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
