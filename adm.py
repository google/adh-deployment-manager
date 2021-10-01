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

logging.getLogger().setLevel(logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("-c|--config", dest="config_path", default="config.yml")
parser.add_argument("-q|--queries-path", dest="queries_path", default="sql")
parser.add_argument("command")
parser.add_argument("subcommand", nargs="?")
args = parser.parse_args()

credentials, _ = google.auth.default()

config = os.path.join(os.getcwd(), args.config_path)
deployment = Deployment(config=config,
                        credentials=credentials,
                        queries_folder=os.path.join(os.getcwd(),
                                                    args.queries_path))

if args.command == "run":
    if args.subcommand == "update":
        deployment.deploy(update=True)
    elif args.subcommand == "deploy":
        deployment.deploy()
    deployment.run()
elif args.command == "deploy":
    update_command = False
    if args.subcommand == "update":
        update_command = True
    deployment.deploy(update=update_command)
elif args.command == "update":
    deployment.update_only()
