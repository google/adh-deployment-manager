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
