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

import logging
import argparse
import os
import google.auth
from adh_deployment_manager.deployment import Deployment
from adh_deployment_manager.commands_factory import CommandsFactory

logging.getLogger().setLevel(logging.INFO)

def execute_command(factory, command, deployment, parameters):
    logging.info(f"Executing `{command}` command...")
    executable_command = factory.create_command(command, deployment)
    executable_command.execute(**parameters)

parser = argparse.ArgumentParser()
parser.add_argument("-c|--config", dest="config_path", default="config.yml")
parser.add_argument("-q|--queries-path", dest="queries_path", default="sql")
parser.add_argument("-l|--location", dest="location", default="sql")
parser.add_argument("command")
parser.add_argument("subcommand", nargs="?")
args = parser.parse_args()


credentials, _ = google.auth.default()
DEVELOPER_KEY = os.environ['ADH_DEVELOPER_KEY']
config = os.path.join(os.getcwd(), args.config_path)
deployment = Deployment(config=config,
                        developer_key=DEVELOPER_KEY,
                        credentials=credentials,
                        queries_folder=os.path.join(os.getcwd(),
                                                    args.queries_path))
extra_parameters = vars(args)
command = extra_parameters["command"]
factory = CommandsFactory()
for command in [args.subcommand, command]:
    if command:
        execute_command(factory, command, deployment, extra_parameters)
