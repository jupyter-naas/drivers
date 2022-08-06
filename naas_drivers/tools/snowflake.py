import sys
import logging
from typing import Dict, List
from pandas import DataFrame
from types import SimpleNamespace

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

        self.api = SimpleNamespace(**{
            'database': Database,
            'schema': Schema,
            'file_format': FileFormat,
            'stage': Stage,
            'warehouse': Warehouse,
            'role': Role
        })

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
            raise TypeError('Wrong type for property [warehouse] - `str` required')

    @database.setter
    def database(self, database: str) -> None:
        if isinstance(database, str):
            self.cursor.execute(f"USE DATABASE {database};")
            self._database = database
        else:
            raise TypeError('Wrong type for property [database] - `str` required')

    @schema.setter
    def schema(self, schema: str) -> None:
        if isinstance(schema, str):
            self.cursor.execute(f"USE SCHEMA {schema};")
            self._schema = schema
        else:
            raise TypeError('Wrong type for property [schema] - `str` required')

    @role.setter
    def role(self, role: str) -> None:
        if isinstance(role, str):
            self.cursor.execute(f"USE ROLE {role};")
            self._role = role
        else:
            raise TypeError('Wrong type for property [role] - `str` required')

    def connect(
        self,
        account: str,
        username: str,
        password: str,
        warehouse: str = "",
        database: str = "",
        schema: str = "",
        role: str = ""
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
            account=account,
            user=username,
            password=password
        )
        self._cursor = self._connection.cursor()
        self._set_environment(warehouse, database, schema, role)
        self.connected = True

    def execute(
        self,
        sql: str,
        warehouse: str = '',
        database: str = '',
        schema: str = '',
        role: str = '',
        n: int = 10,
        return_statement: bool = False
    ) -> Dict:
        """
        Execute passed command. Could be anything, starting from DQL query, and ending with DDL commands
        @param sql: command/query to execute
        @param warehouse: (optional) warehouse to use for passed command
        @param database: (optional) database to use for passed command
        @param schema: (optional) schema to use for passed command
        @param role: (optional) role to use for passed command
        @param n: (optional) query result length limit
        @param return_statement: (optional) whether to return generated statement
        @return: List: (results, columns_metadata) containing query outcome
        """
        warehouse_old, database_old, schema_old, role_old = self.warehouse, self.database, self.schema, self.role

        # If applicable (any param has been changed), switch environment for a single command execution
        alter_environment = any([env_elem != '' for env_elem in [warehouse, database, schema, role]])
        if alter_environment:
            self._set_environment(warehouse, database, schema, role)

        res = self._cursor.execute(sql)
        result_dict = {
            'results': res.fetchall() if n == -1 else res.fetchmany(n),
            'description': res.description,
            'statement': sql if return_statement else ''
        }

        if alter_environment:
            self._set_environment(warehouse_old, database_old, schema_old, role_old)

        return result_dict

    def query(
        self,
        sql: str,
        warehouse: str = '',
        database: str = '',
        schema: str = '',
        role: str = '',
        n: int = 10,
        return_statement: bool = False
    ) -> Dict:
        """
        Query data and return results in the form of plain vanilla List: (results, columns_metadata)
        @param sql: query to execute
        @param warehouse: (optional) warehouse to use for passed query
        @param database: (optional) database to use for passed query
        @param schema: (optional) schema to use for passed query
        @param role: (optional) role to use for passed query
        @param n: (optional) query result length limit
        @param return_statement: (optional) whether to return generated statement
        @return: List: (results, columns_metadata) containing query outcome
        """
        return self.execute(sql, warehouse, database, schema, role, n, return_statement)

    def query_pd(
        self,
        sql: str,
        warehouse: str = '',
        database: str = '',
        schema: str = '',
        role: str = '',
        n: int = 10,
        return_statement: bool = False
    ) -> DataFrame:
        """
        Query data and return results in the form of pandas.DataFrame
        @param sql: query to execute
        @param warehouse: (optional) warehouse to use for passed query
        @param database: (optional) database to use for passed query
        @param schema: (optional) schema to use for passed query
        @param role: (optional) role to use for passed query
        @param n: (optional) query result length limit
        @param return_statement: (optional) whether to return generated statement
        @return: pandas.DataFrame table containing query outcome
        """
        res = self.query(sql, warehouse, database, schema, role, n, return_statement)

        # TODO: Apply dtypes mapping from ResultMetadata objects
        return DataFrame(res['results'], columns=[result_metadata.name for result_metadata in res['description']])

    def copy_into(
        self,
        table_name: str,
        source_stage: str,
        transformed_columns: str = '',
        files: List[str] = None,
        regex_pattern: str = '',
        file_format_name: str = '',
        validation_mode: str = '',
        return_statement: bool = False,
        **kwargs
    ) -> Dict:
        """
        Copy data from stage to Snowflake table
        As this function contains a lot of nuances, please refer to the documentation:
            https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
        @param table_name: the name of the table into which data is loaded. Optionally, can specify namespace too
            (i.e., <database_name>.<schema_name> or <schema_name> if database is already selected)
        @param source_stage: internal or external location where the files containing data to be loaded are staged
            caution: it can be also a SELECT statement so data from stage is transformed
        @param transformed_columns: if `source_stage` is a SELECT statement, it's possible to
        @param files: a list of one or more files names (in an array) to be loaded
        @param regex_pattern: a regular expression pattern for filtering files from the output
        @param file_format_name: the format of the data files to load (so far only name is accepted as an input)
        @param validation_mode: instructs the COPY command to validate the data files
            instead of loading them into the specified table.
            Caution: does not support COPY statements that transform data during a load.
            If applied along with transformation SELECT statement, it will throw error on Snowflake side
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "COPY INTO" \
                    f" {table_name}"

        # If applicable, then data load with transformation has been scheduled
        # If not, it's a standard data load
        if source_stage.upper().strip().startswith('SELECT'):
            statement += f"{' ' + transformed_columns if transformed_columns != '' else ''}" \
                         f" FROM ({source_stage})"
        else:
            statement += f" FROM {source_stage}"

        files_string = ",".join([f"'{file}'" for file in files]) if files is not None else ""
        statement += f"{f' ({files_string})' if files_string != '' else ''}"

        statement += f"{f' PATTERN = {regex_pattern}' if regex_pattern != '' else ''}" \
                     f"{f' FILE FORMAT = (FORMAT_NAME = {file_format_name})' if file_format_name != '' else ''}" \
                     f"{f' VALIDATION_MODE = {validation_mode}' if validation_mode != '' else ''}"

        # looping through kwargs for extra arguments passed in statement
        # while executing final command, Snowflake will do the validation
        for key, value in kwargs.items():
            statement += f" {key} = {value}"

        return self.execute(statement, n=1, return_statement=return_statement)

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
        self,
        warehouse: str = '',
        database: str = '',
        schema: str = '',
        role: str = ''
    ) -> None:
        """
        Tries to set Snowflake environment in bulk: warehouse, database, schema, and role
        @param warehouse: warehouse to set in the environment
        @param database: database to set in the environment
        @param schema: schema to set in the environment
        @param role: role to set in the environment
        """
        warehouse, database, schema, role = warehouse.strip(), database.strip(), schema.strip(), role.strip()
        try:
            if warehouse != '':
                self.warehouse = warehouse
            if database != '':
                self.database = database
            if schema != '':
                self.schema = schema
            if role != '':
                self.role = role
        except ProgrammingError as pe:
            logging.error(f'Error while setting SF environment. More on that: {pe}')
            sys.exit()


class Database:

    @staticmethod
    def use(
        database_name: str,
        return_statement: bool = False
    ) -> Dict:
        """
        @param database_name: name of the database to use
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "USE" \
                    f" DATABASE {database_name}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)

    @staticmethod
    def create(
        database_name: str,
        or_replace: bool = False,
        return_statement: bool = False
    ) -> Dict:
        """
        Executes command to create a Snowflake database with a given name
        @param database_name: database name to create
        @param or_replace: replace schema if exists
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "CREATE" \
                    f"{' OR REPLACE' if or_replace else ''}" \
                    f" DATABASE {database_name}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)

    @staticmethod
    def drop(
        database_name: str,
        if_exists: bool = False,
        return_statement: bool = False
    ) -> Dict:
        """
        Executes command to drop a Snowflake database with a given name
        @param database_name: database name to drop
        @param if_exists: adds `IF EXISTS` statement to a command
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "DROP DATABASE" \
                    f"{' IF EXISTS' if if_exists else ''}" \
                    f" {database_name}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)


class Schema:

    @staticmethod
    def use(
        schema_name: str,
        return_statement: bool = False
    ) -> Dict:
        """
        @param schema_name: name of the schema to use
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "USE" \
                    f" SCHEMA {schema_name}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)

    @staticmethod
    def create(
        schema_name: str,
        or_replace: bool = False,
        return_statement: bool = False
    ) -> Dict:
        """
        Executes command to create a Snowflake schema with a given name
        @param schema_name: schema name to create
        @param or_replace: replace schema if exists
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "CREATE" \
                    f"{' OR REPLACE' if or_replace else ''}" \
                    f" SCHEMA {schema_name}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)

    @staticmethod
    def drop(
        schema_name: str,
        if_exists: bool = False,
        return_statement: bool = False
    ) -> Dict:
        """
        Executes command to drop a Snowflake schema with a given name
        @param schema_name: schema name to drop
        @param if_exists: adds `IF EXISTS` statement to a command
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "DROP SCHEMA" \
                    f"{' IF EXISTS' if if_exists else ''}" \
                    f" {schema_name}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)


class FileFormat:

    AVAILABLE_FORMAT_TYPES = ['CSV', 'JSON', 'AVRO', 'ORC', 'PARQUET', 'XML']

    @staticmethod
    def create(
        file_format_name: str,
        file_format_type: str,
        or_replace: bool = False,
        if_not_exists: bool = False,
        return_statement: bool = False,
        **kwargs
    ) -> Dict:
        """
        Executes command to create a Snowflake file format with a given name
        @param file_format_name: file format name to create
        @param file_format_type: type of the file format to be created
        @param or_replace: replace file format if exists
        @param if_not_exists: create object if it doesn't exist so far
        @param return_statement: whether to return generated statement
        @param kwargs: additional arguments to be passed to the statement
            so far validation is on the Snowflake engine side
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        file_format_type = file_format_type.upper().strip()
        if file_format_type not in FileFormat.AVAILABLE_FORMAT_TYPES:
            raise ValueError(f'File Format type `{file_format_type}` not available for now')

        statement = "CREATE" \
                    f"{' OR REPLACE' if or_replace else ''}" \
                    f" FILE FORMAT{' IF NOT EXISTS' if if_not_exists else ''} {file_format_name}" \
                    f" TYPE = {file_format_type}"

        # looping through kwargs for extra arguments passed in statement
        # while executing final command, Snowflake will do the validation
        for key, value in kwargs.items():
            statement += f" {key} = {value}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)

    @staticmethod
    def drop(
        file_format_name: str,
        if_exists: bool = False,
        return_statement: bool = False
    ) -> Dict:
        """
        Executes command to drop a Snowflake file format with a given name
        @param file_format_name: file format name to drop
        @param if_exists: adds `IF EXISTS` statement to a command
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "DROP FILE FORMAT" \
                    f"{' IF EXISTS' if if_exists else ''}" \
                    f" {file_format_name}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)


class Stage:

    @staticmethod
    def create(
        stage_name: str,
        or_replace: bool = False,
        is_temporary: bool = False,
        if_not_exists: bool = False,
        file_format_name: str = '',
        return_statement: bool = False,
        **kwargs
    ) -> Dict:
        """
        Executes command to create a Snowflake stage with a given name
        @param stage_name: stage name to create
        @param or_replace: replace file format if exists
        @param is_temporary: create a temporary stage
        @param if_not_exists: create object if it doesn't exist so far
        @param file_format_name: file format name to use while creating a stage
        @param return_statement: whether to return generated statement
        @param kwargs: additional arguments to be passed to the statement
            so far validation is on the Snowflake engine side
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "CREATE" \
                    f"{' OR REPLACE' if or_replace else ''}" \
                    f"{' TEMPORARY' if is_temporary else ''}" \
                    f" STAGE" \
                    f"{' IF NOT EXISTS' if if_not_exists else ''}" \
                    f" {stage_name}" \
                    f"{f' FILE_FORMAT = {file_format_name}' if file_format_name != '' else ''}"

        # looping through kwargs for extra arguments passed in statement
        # while executing final command, Snowflake will do the validation
        for key, value in kwargs.items():
            statement += f" {key} = {value}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)

    @staticmethod
    def drop(
        stage_name: str,
        if_exists: bool = False,
        return_statement: bool = False
    ) -> Dict:
        """
        Executes command to drop a Snowflake stage with a given name
        @param stage_name: stage name to drop
        @param if_exists: adds `IF EXISTS` statement to a command
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "DROP STAGE" \
                    f"{' IF EXISTS' if if_exists else ''}" \
                    f" {stage_name}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)

    @staticmethod
    def put(
        filepath: str,
        internal_stage_name: str,
        parallel: int = 4,
        auto_compress: bool = True,
        source_compression: str = 'AUTO_DETECT',
        overwrite: bool = False,
        return_statement: bool = False
    ) -> Dict:
        """
        Executes command to put data to Snowflake internal stage from a local machine
        @param filepath: local path to a file
        @param internal_stage_name: stage name where to put file a file
            Caution: when having problems with this param, please check whether you specify the proper
            internal stage name, including special signs: @, %, and ~
        @param parallel: number of threads to use for uploading files
        @param auto_compress: whether Snowflake uses gzip to compress files during upload
        @param source_compression: method of compression used on already-compressed files that are being staged
        @param overwrite: whether Snowflake overwrites an existing file with the same name during upload
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "PUT" \
                    f" '{filepath}'" \
                    f" {internal_stage_name}" \
                    f" PARALLEL = {parallel}" \
                    f" AUTO_COMPRESS = {'TRUE' if auto_compress else 'FALSE'}" \
                    f" SOURCE_COMPRESSION = {source_compression}" \
                    f" OVERWRITE = {'TRUE' if overwrite else 'FALSE'}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)

    @staticmethod
    def list(
        stage_name: str,
        regex_pattern: str = '',
        return_statement: bool = False
    ) -> Dict:
        """
        Lists files that are inside in a particular stage
        @param stage_name: the location where the data files are staged
        @param regex_pattern: a regular expression pattern for filtering files from the output
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "LIST" \
                    f" {stage_name}" \
                    f"{f' PATTERN = {regex_pattern}' if regex_pattern != '' else ''}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)


class Role:

    @staticmethod
    def use(
        role_name: str,
        return_statement: bool = False
    ) -> Dict:
        """
        @param role_name: name of the role to use
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "USE" \
                    f" ROLE {role_name}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)


class Warehouse:

    @staticmethod
    def use(
        warehouse_name: str,
        return_statement: bool = False
    ) -> Dict:
        """
        @param warehouse_name: name of the warehouse to use
        @param return_statement: whether to return generated statement
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "USE" \
                    f" WAREHOUSE {warehouse_name}"

        return snowflake_instance.execute(statement, n=1, return_statement=return_statement)
