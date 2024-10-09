import snowflake.connector

from .client import Client


class DataAccess:
    def __init__(self):
        self._creds = None

    def query(self, sql):

        if self._creds is None:
            self._creds = Client()._get_data_access_credentials()

        with snowflake.connector.connect(
            account=self._creds.account.get_secret_value(),
            database="ANALYTICS_DB",
            user=self._creds.user.get_secret_value(),
            password=self._creds.password.get_secret_value(),
        ) as conn:
            df = conn.cursor().execute(sql).fetch_pandas_all()
            df.columns = [x.lower() for x in df.columns]

            return df
