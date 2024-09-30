import os

import snowflake.connector


class DataAccess:
    def __init__(self):
        self._authenticated = False
        self._default_warehouse = "ADMIN_WH"

    def authenticate(self):
        self._authenticated = True

    def query(self, database, sql, warehouse=None):
        if warehouse is None:
            warehouse = self._default_warehouse

        with snowflake.connector.connect(
            user=os.environ.get("CECIL_SNOWFLAKE_USER"),
            account=os.environ.get("CECIL_SNOWFLAKE_ACCOUNT"),
            private_key_file=os.environ.get("CECIL_SNOWFLAKE_PRIVATE_KEY_FILE"),
            database=database,
            warehouse=warehouse,
        ) as conn:
            return conn.cursor().execute(sql).fetch_pandas_all()
