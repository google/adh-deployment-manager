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

import os
import logging
import pickle
from oauth2client.service_account import ServiceAccountCredentials  # type: ignore
from google_auth_oauthlib import flow  # type: ignore
import google.auth  # type: ignore

_SCOPE = "https://www.googleapis.com/auth/adsdatahub"


class BaseAuthenticator:
    def __init__(self, successor):
        self.successor = successor

    def handle(self, file):
        if self.successor:
            return self.successor.handle(file)


class ServiceAccount(BaseAuthenticator):
    def handle(self, file):
        try:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                file, _SCOPE)
            return credentials
        except:
            return super().handle(file)


class InstalledAppFlow(BaseAuthenticator):
    def handle(self, file):
        try:
            appflow = flow.InstalledAppFlow.from_client_secrets_file(
                file, _SCOPE)
            appflow.run_console()
            credentials = appflow.credentials
            return credentials
        except:
            return super().handle(file)


class DefaultCredentials(BaseAuthenticator):
    def handle(self, file):
        try:
            credentials, _ = google.auth.default()
            return credentials
        except:
            return super().handle(file)


class AdhAutheticator:
    def __init__(self):
        self.authenticator = self._init_authenticators()

    def _init_authenticators(self):
        authenticator_chain = BaseAuthenticator(None)
        for auth in DefaultCredentials, ServiceAccount, InstalledAppFlow:
            new_authenticator = auth(authenticator_chain)
            authenticator_chain = new_authenticator
        return authenticator_chain

    def get_credentials(self, file, dump_to_file=True):
        credentials = self.check_local_credentials(
        ) or self.authenticator.handle(file)
        if dump_to_file:
            self.dump_credentials_to_file(credentials)
        return credentials

    def check_local_credentials(self):
        if os.path.exists("token.pickle"):
            logging.debug("reading credentials")
            with open("token.pickle", "rb") as token:
                local_credentials = pickle.load(token)
            return None if local_credentials.expired else local_credentials

    def dump_credentials_to_file(self, credentials):
        with open("token.pickle", "wb") as token:
            logging.debug("saving credentials")
            pickle.dump(credentials, token)
