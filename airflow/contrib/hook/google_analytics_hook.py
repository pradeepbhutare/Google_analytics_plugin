"""
There are two ways to authenticate the Google Analytics Hook.

If you have already obtained an OAUTH token, place it in the password field
of the relevant connection.

If you don't have an OAUTH token, you may authenticate by passing a
'client_secrets' object to the extras section of the relevant connection. This
object will expect the following fields and use them to generate an OAUTH token
on execution.

"type": "service_account",
"project_id": "example-project-id",
"private_key_id": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
"private_key": "-----BEGIN PRIVATE KEY-----\nXXXXX\n-----END PRIVATE KEY-----\n",
"client_email": "google-analytics@{PROJECT_ID}.iam.gserviceaccount.com",
"client_id": "XXXXXXXXXXXXXXXXXXXXXX",
"auth_uri": "https://accounts.google.com/o/oauth2/auth",
"token_uri": "https://accounts.google.com/o/oauth2/token",
"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
"client_x509_cert_url": "{CERT_URL}"

More details can be found here:
https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
"""

import time
from apiclient.discovery import build
from airflow.exceptions import AirflowException

from oauth2client.service_account import ServiceAccountCredentials
from airflow.hooks.base_hook import BaseHook


class GoogleAnalyticsHook(BaseHook):
    def __init__(self, google_analytics_conn_id='test_ga', delegate_to=None):
        super(GoogleAnalyticsHook, self).__init__(google_analytics_conn_id)
        self.google_analytics_conn_id = google_analytics_conn_id
        self.connection = self.get_connections(self.google_analytics_conn_id)
        self.delegate_to = delegate_to




    def get_service_object(self):

        json_file = self.connection[0].login
        self.log.info('Json file  %s', json_file)
        scope = self.connection[0].extra
        service_name = self.connection[0].schema
        version = self.connection[0].host
        credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file, scopes=scope)
        service_object = build(service_name, version, credentials=credentials, cache_discovery=False)
        return service_object






