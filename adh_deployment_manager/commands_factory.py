from inspect import getmembers, isclass, isabstract, getfile
from pathlib import Path
import adh_deployment_manager.commands as commands

class CommandsFactory:
    commands = {}

    def __init__(self):
        self.load_commands()

    def _print_commands(self):
        for command in self.commands:
            print(command)

    def load_commands(self):
        classes = getmembers(commands,
                             lambda m: isclass(m) and not isabstract(m))
        for _, _type in classes:
            if isclass(_type) and issubclass(_type, commands.AbsCommand):
                command = Path(getfile(_type)).stem
                self.commands.update([[command, _type]])

    def create_command(self, command, deployment):
        if command in self.commands:
            return self.commands[command](deployment)
        else:
            return commands.NullCommand(command)
