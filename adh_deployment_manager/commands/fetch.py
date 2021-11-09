from .abs_command import AbsCommand

class Fetcher(AbsCommand):
    def __init__(self,
                 deployment):
        self.deployment = deployment

    def execute(self, location, file_name=None, extension=".sql", **kwargs):
        queries = self.deployment._get_queries()
        for _, analysis_query in queries:
                analysis_query.dump(location, file_name, extension)
