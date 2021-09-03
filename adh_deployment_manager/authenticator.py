import logging
import pickle

_SCOPE = "https://www.googleapis.com/auth/adsdatahub"


class authenticator:
    def __init__(self, file, scope=_SCOPE):
        self.secrets_file = file
        self.scope = scope
        self.credentials = self._get_credentials(
        ) if force_refresh else self.read_credentials(
        ) or self._get_credentials()

    def _get_credentials(self):
        pass

    def dump(self, credentials):
        with open("token.pickle", "wb") as token:
            logging.debug("saving credentials")
            pickle.dump(credentials, token)

    def read_credentials(self):
        import os
        if not os.path.exists("token.pickle"):
            return None
        logging.debug("reading credentials")
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)
            if credentials.expired:
                self._get_credentials()
        return credentials


class ServiceAccount(authenticator):
    def __init__(self, service_account_file, scope=_SCOPE):
        self.secrets_file = service_account_file
        self.scope = scope
        self.credentials = self._get_credentials(
        ) if force_refresh else self.read_credentials(
        ) or self._get_credentials()

    def _get_credentials(self):
        from oauth2client.service_account import ServiceAccountCredentials  # type: ignore
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.service_account_file, _SCOPE)
        return credentials


class InstalledAppFlow(authenticator):
    def __init__(self, client_secrets_file, scope=_SCOPE, force_refresh=False):
        self.secrets_file = client_secrets_file
        self.scope = scope
        self.credentials = self._get_credentials() if force_refresh else super(
        ).read_credentials() or self._get_credentials()

    def _get_credentials(self):
        logging.debug("getting credentials")
        from google_auth_oauthlib import flow
        appflow = flow.InstalledAppFlow.from_client_secrets_file(
            self.secrets_file, self.scope)
        appflow.run_local_server()
        credentials = appflow.credentials
        super().dump(credentials)
        return credentials
