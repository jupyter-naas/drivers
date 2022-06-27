from typing import List
import logging
import sys
from pandas import DataFrame

import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from naas_drivers.driver import InDriver, OutDriver


global snowflake_instance


class Snowflake(InDriver, OutDriver):
    """
    Driver for Snowflake Data Warehouse. Enables to connect to the SF account and execute queries.
    There is no need to log in and use Worksheets. Connection instance is enough to run any command.

    Basic drill recommendation:
        - `snowflake.connect()` - to connect to the account with given credentials
        - `snowflake.<warehouse/schema/database>()` - to eventually set up SF working environment for the whole session
        - `snowflake.execute()` - to run any command you want
        - `snowflake.query() / snowflake.query_pd()` - additional, more convenient functionality to run DQL queries,
            and, eventually, return data in form of the pandas.DataFrame.

    For more information, please refer to functions' documentation.

    Moreover, there are 2 extra session's objects accessible: `cursor` and `connection` that are raw
        `snowflake-python-connector` objects. If you need, you can refer to Snowflake docs and perform extra tasks that
        aren't (yet) implemented in this driver.

    If the functionality you're looking for is not implemented yet, please create an issue in
        (naas-drivers repository)[https://github.com/jupyter-naas/drivers/tree/main],
        we would be more than happy to help.
    """

    def __init__(self):

        self._connection = None
        self._cursor = None
        self._warehouse = None
        self._database = None
        self._schema = None
        self._role = None

        global snowflake_instance
        snowflake_instance = self

    @staticmethod
    def instance():
        return snowflake_instance

    @property
    def connection(self):
        return self._connection

    @property
    def cursor(self):
        return self._cursor

    @property
    def warehouse(self) -> str:
        return self._warehouse

    @property
    def database(self) -> str:
        return self._database

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def role(self) -> str:
        return self._role

    @warehouse.setter
    def warehouse(self, warehouse: str) -> None:
        if isinstance(warehouse, str):
            self.cursor.execute(f"USE WAREHOUSE {warehouse};")
            self._warehouse = warehouse
        else:
            raise TypeError("Wrong type for property [warehouse] - `str` required")

    @database.setter
    def database(self, database: str) -> None:
        if isinstance(database, str):
            self.cursor.execute(f"USE DATABASE {database};")
            self._database = database
        else:
            raise TypeError("Wrong type for property [database] - `str` required")

    @schema.setter
    def schema(self, schema: str) -> None:
        if isinstance(schema, str):
            self.cursor.execute(f"USE SCHEMA {schema};")
            self._schema = schema
        else:
            raise TypeError("Wrong type for property [schema] - `str` required")

    @role.setter
    def role(self, role: str) -> None:
        if isinstance(role, str):
            self.cursor.execute(f"USE ROLE {role};")
            self._role = role
        else:
            raise TypeError("Wrong type for property [role] - `str` required")

    def connect(
        self,
        account: str,
        username: str,
        password: str,
        warehouse: str = "",
        database: str = "",
        schema: str = "",
        role: str = "",
    ) -> None:
        """
        Connects to Snowflake account with given credentials.
        Connection is established and both `connection` and `cursor` are being set up
        @param account: SF account identifier, can be fetched from login URL,
            e.g. <account_identifier>.snowflakecomputing.com
        @param username: SF account username
        @param password: SF account password
        @param warehouse: (optional) SF warehouse to set up while creating a connection
        @param database: (optional) SF database to set up while creating a connection
        @param schema: (optional) SF schema to set up while creating a connection
        @param role: (optional) SF role to set up while creating a connection
        """
        self._connection = snowflake.connector.connect(
            account=account, user=username, password=password
        )
        self._cursor = self._connection.cursor()
        self._set_environment(warehouse, database, schema, role)
        self.connected = True

    def execute(
        self,
        sql: str,
        warehouse: str = "",
        database: str = "",
        schema: str = "",
        role: str = "",
        n: int = 10,
    ) -> (List, List):
        """
        Execute passed command. Could be anything, starting from DQL query, and ending with DDL commands
        @param sql: command/query to execute
        @param warehouse: (optional) warehouse to use for passed command
        @param database: (optional) database to use for passed command
        @param schema: (optional) schema to use for passed command
        @param role: (optional) role to use for passed command
        @param n: (optional) query result length limit
        @return: List: (results, columns_metadata) containing query outcome
        """

        # Switching warehouse, database, schema, and role for function call purposes
        warehouse_old, database_old, schema_old, role_old = (
            self.warehouse,
            self.database,
            self.schema,
            self.role,
        )
        self._set_environment(warehouse, database, schema, role)
        self.warehouse, self.database, self.schema, self.role = (
            warehouse_old,
            database_old,
            schema_old,
            role_old,
        )

        res = self._cursor.execute(sql)
        if res.rowcount < n:
            n = res.rowcount
        if n > 0:
            return res.fetchmany(n), res.description
        if n == -1:
            return res.fetchall(), res.description

    def query(
        self,
        sql: str,
        warehouse: str = "",
        database: str = "",
        schema: str = "",
        role: str = "",
        n: int = 10,
    ) -> (List, List):
        """
        Query data and return results in the form of plain vanilla List: (results, columns_metadata)
        @param sql: query to execute
        @param warehouse: (optional) warehouse to use for passed query
        @param database: (optional) database to use for passed query
        @param schema: (optional) schema to use for passed query
        @param role: (optional) role to use for passed query
        @param n: (optional) query result length limit
        @return: List: (results, columns_metadata) containing query outcome
        """
        return self.execute(sql, warehouse, database, schema, role, n)

    def query_pd(
        self,
        sql: str,
        warehouse: str = "",
        database: str = "",
        schema: str = "",
        role: str = "",
        n: int = 10,
    ) -> DataFrame:
        """
        Query data and return results in the form of pandas.DataFrame
        @param sql: query to execute
        @param warehouse: (optional) warehouse to use for passed query
        @param database: (optional) database to use for passed query
        @param schema: (optional) schema to use for passed query
        @param role: (optional) role to use for passed query
        @param n: (optional) query result length limit
        @return: pandas.DataFrame table containing query outcome
        """
        res = self.query(sql, warehouse, database, schema, role, n)

        # TODO: Apply dtypes mapping from ResultMetadata objects
        return DataFrame(
            res[0], columns=[result_metadata.name for result_metadata in res[1]]
        )

    def close_connection(self) -> None:
        """
        Closes a connection to Snowflake account, resets all the internal parameters
        """
        self._cursor.close()
        self._connection.close()

        self._schema = None
        self._database = None
        self._cursor = None
        self._connection = None

    def _set_environment(
        self, warehouse: str = "", database: str = "", schema: str = "", role: str = ""
    ) -> None:
        """
        Tries to set Snowflake environment in bulk: warehouse, database, schema, and role
        @param warehouse: warehouse to set in the environment
        @param database: database to set in the environment
        @param schema: schema to set in the environment
        @param role: role to set in the environment
        """
        warehouse, database, schema, role = (
            warehouse.strip(),
            database.strip(),
            schema.strip(),
            role.strip(),
        )
        try:
            if warehouse != "":
                self.warehouse = warehouse
            if database != "":
                self.database = database
            if schema != "":
                self.schema = schema
            if role != "":
                self.role = role
        except ProgrammingError as pe:
            logging.error(f"Error while setting SF environment. More on that: {pe}")
            sys.exit()
