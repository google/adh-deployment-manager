from .abs_command import AbsCommand

class NullCommand(AbsCommand):

    def __init__(self, command):
        self.command = command

    def execute(self, **kwargs):
        print(f"Unknown command {self.command}")
