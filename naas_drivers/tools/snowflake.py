import sys
import logging
from typing import Dict, List, Optional
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
        - `snowflake.set_environment()` - to set up SF working environment for the whole session
        - `snowflake.execute()` or `snowflake.query_pd()` - to run any command you want
            (and eventually get the results in a nice Pandas DataFrame)

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

        # Creating Snowflake objects
        self._database = Database(self)
        self._file_format = FileFormat(self)
        self._role = Role(self)
        self._schema = Schema(self)
        self._stage = Stage(self)
        self._storage_integration = StorageIntegration(self)
        self._warehouse = Warehouse(self)

        # Providing global instance to use after loading snowflake driver module
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
    def database(self):
        return self._database

    @property
    def file_format(self):
        return self._file_format

    @property
    def role(self):
        return self._role

    @property
    def schema(self):
        return self._schema

    @property
    def stage(self):
        return self._stage

    @property
    def storage_integration(self):
        return self._storage_integration

    @property
    def warehouse(self):
        return self._warehouse

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
        @param warehouse: SF warehouse to set up while creating a connection
        @param database: SF database to set up while creating a connection
        @param schema: SF schema to set up while creating a connection
        @param role: SF role to set up while creating a connection
        """
        self._connection = snowflake.connector.connect(
            account=account,
            user=username,
            password=password
        )
        self._cursor = self._connection.cursor()
        self.set_environment(warehouse, database, schema, role)
        self.connected = True

    def execute(
        self,
        sql: str,
        n: int = 10,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Execute passed command. Could be anything, starting from DQL query, and ending with DDL commands
        @param sql: command/query to execute
        @param n: query result length limit
        @param silent: whether to return result dictionary with multiple information or not (run in silent mode)
        @return: dictionary containing query information outcome (results, columns_metadata, and sql statement)
        """
        res = self._cursor.execute(sql)

        if silent:
            return None
        else:
            return {
                'results': res.fetchall() if n == -1 else res.fetchmany(n),
                'description': res.description,
                'statement': sql
            }

    def query_pd(
        self,
        sql: str,
        n: int = 10
    ) -> DataFrame:
        """
        Query data and return results in the form of pandas.DataFrame
        @param sql: query to execute
        @param n: query result length limit
        @return: pandas.DataFrame table containing query outcome
        """
        res = self.execute(sql, n)

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
        silent: bool = False,
        **kwargs
    ) -> Optional[Dict]:
        """
        Copy data from stage to Snowflake table
        As this function contains a lot of nuances, please refer to the documentation:
            https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
        @param table_name: the name of the table into which data is loaded. Optionally, can specify namespace too
            (i.e., <database_name>.<schema_name> or <schema_name> if database is already selected)
        @param source_stage: internal or external location where the files containing data to be loaded are staged
            caution: it can be also a SELECT statement so data from stage is transformed
        @param transformed_columns: if `source_stage` is a SELECT statement,
            specifies an explicit set of fields/columns (separated by commas) to load from the staged data files
        @param files: a list of one or more files names (in an array) to be loaded
        @param regex_pattern: a regular expression pattern for filtering files from the output
        @param file_format_name: the format of the data files to load (so far only name is accepted as an input)
        @param validation_mode: instructs the COPY command to validate the data files
            instead of loading them into the specified table.
            Caution: does not support COPY statements that transform data during a load.
            If applied along with transformation SELECT statement, it will throw error on Snowflake side
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
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

        return self.execute(statement, n=1, silent=silent)

    def close_connection(self) -> None:
        """
        Closes a connection to Snowflake account, resets all the internal parameters
        """
        self._cursor.close()
        self._connection.close()

        self._cursor = None
        self._connection = None

    def get_environment(
        self
    ) -> Dict:
        """
        Returns current session environment that consists of:
            - role
            - database
            - schema
            - warehouse
        @return: dictionary of current session environment elements
        """

        return {
            'role': self._role.get_current(),
            'database': self._database.get_current(),
            'schema': self._schema.get_current(),
            'warehouse': self._warehouse.get_current()
        }

    def set_environment(
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
                self._warehouse.use(warehouse)
            if database != '':
                self._database.use(database)
            if schema != '':
                self._schema.use(schema)
            if role != '':
                self._role.use(role)
        except ProgrammingError as pe:
            logging.error(f'Error while setting SF environment. More on that: {pe}')
            sys.exit()


class Database:

    def __init__(
        self,
        snowflake_driver: Snowflake
    ):
        self.__snowflake_driver = snowflake_driver

    def use(
        self,
        database_name: str,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Sets particular database for a session
        @param database_name: name of the database to use
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "USE" \
                    f" DATABASE {database_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def get_current(
        self,
    ) -> str:
        """
        Returns the name of the database in use by the current session
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "SELECT CURRENT_DATABASE()"

        return self.__snowflake_driver.execute(statement, n=1)['results'][0][0]

    def create(
        self,
        database_name: str,
        or_replace: bool = False,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Executes command to create a Snowflake database with a given name
        @param database_name: database name to create
        @param or_replace: replace schema if exists
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "CREATE" \
                    f"{' OR REPLACE' if or_replace else ''}" \
                    f" DATABASE {database_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def drop(
        self,
        database_name: str,
        if_exists: bool = False,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Executes command to drop a Snowflake database with a given name
        @param database_name: database name to drop
        @param if_exists: adds `IF EXISTS` statement to a command
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "DROP DATABASE" \
                    f"{' IF EXISTS' if if_exists else ''}" \
                    f" {database_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)


class Schema:

    def __init__(
        self,
        snowflake_driver: Snowflake
    ):
        self.__snowflake_driver = snowflake_driver

    def use(
        self,
        schema_name: str,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Sets particular schema for a session
        @param schema_name: name of the schema to use
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "USE" \
                    f" SCHEMA {schema_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def get_current(
        self,
    ) -> str:
        """
        Returns the name of the schema in use by the current session
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "SELECT CURRENT_SCHEMA()"

        return self.__snowflake_driver.execute(statement, n=1)['results'][0][0]

    def create(
        self,
        schema_name: str,
        or_replace: bool = False,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Executes command to create a Snowflake schema with a given name
        @param schema_name: schema name to create
        @param or_replace: replace schema if exists
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "CREATE" \
                    f"{' OR REPLACE' if or_replace else ''}" \
                    f" SCHEMA {schema_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def drop(
        self,
        schema_name: str,
        if_exists: bool = False,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Executes command to drop a Snowflake schema with a given name
        @param schema_name: schema name to drop
        @param if_exists: adds `IF EXISTS` statement to a command
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "DROP SCHEMA" \
                    f"{' IF EXISTS' if if_exists else ''}" \
                    f" {schema_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)


class FileFormat:

    def __init__(
        self,
        snowflake_driver: Snowflake
    ):
        self.__snowflake_driver = snowflake_driver
        self.__AVAILABLE_FORMAT_TYPES = ['CSV', 'JSON', 'AVRO', 'ORC', 'PARQUET', 'XML']

    def create(
        self,
        file_format_name: str,
        file_format_type: str,
        or_replace: bool = False,
        if_not_exists: bool = False,
        silent: bool = False,
        **kwargs
    ) -> Optional[Dict]:
        """
        Executes command to create a Snowflake file format with a given name
        @param file_format_name: file format name to create
        @param file_format_type: type of the file format to be created
        @param or_replace: replace file format if exists
        @param if_not_exists: create object if it doesn't exist so far
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @param kwargs: additional arguments to be passed to the statement
            so far validation is on the Snowflake engine side
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        file_format_type = file_format_type.upper().strip()
        if file_format_type not in self.__AVAILABLE_FORMAT_TYPES:
            raise ValueError(f'File Format type `{file_format_type}` not available for now')

        statement = "CREATE" \
                    f"{' OR REPLACE' if or_replace else ''}" \
                    f" FILE FORMAT{' IF NOT EXISTS' if if_not_exists else ''} {file_format_name}" \
                    f" TYPE = {file_format_type}"

        # looping through kwargs for extra arguments passed in statement
        # while executing final command, Snowflake will do the validation
        for key, value in kwargs.items():
            statement += f" {key} = {value}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def drop(
        self,
        file_format_name: str,
        if_exists: bool = False,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Executes command to drop a Snowflake file format with a given name
        @param file_format_name: file format name to drop
        @param if_exists: adds `IF EXISTS` statement to a command
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "DROP FILE FORMAT" \
                    f"{' IF EXISTS' if if_exists else ''}" \
                    f" {file_format_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)


class Stage:

    def __init__(
        self,
        snowflake_driver: Snowflake
    ):
        self.__snowflake_driver = snowflake_driver

    def create(
        self,
        stage_name: str,
        or_replace: bool = False,
        is_temporary: bool = False,
        if_not_exists: bool = False,
        file_format_name: str = '',
        silent: bool = False,
        **kwargs
    ) -> Optional[Dict]:
        """
        Executes command to create a Snowflake stage with a given name
        @param stage_name: stage name to create
        @param or_replace: replace file format if exists
        @param is_temporary: create a temporary stage
        @param if_not_exists: create object if it doesn't exist so far
        @param file_format_name: file format name to use while creating a stage
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
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

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def drop(
        self,
        stage_name: str,
        if_exists: bool = False,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Executes command to drop a Snowflake stage with a given name
        @param stage_name: stage name to drop
        @param if_exists: adds `IF EXISTS` statement to a command
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "DROP STAGE" \
                    f"{' IF EXISTS' if if_exists else ''}" \
                    f" {stage_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def put(
        self,
        filepath: str,
        internal_stage_name: str,
        parallel: int = 4,
        auto_compress: bool = True,
        source_compression: str = 'AUTO_DETECT',
        overwrite: bool = False,
        silent: bool = False
    ) -> Optional[Dict]:
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
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "PUT" \
                    f" '{filepath}'" \
                    f" {internal_stage_name}" \
                    f" PARALLEL = {parallel}" \
                    f" AUTO_COMPRESS = {'TRUE' if auto_compress else 'FALSE'}" \
                    f" SOURCE_COMPRESSION = {source_compression}" \
                    f" OVERWRITE = {'TRUE' if overwrite else 'FALSE'}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def list(
        self,
        stage_name: str,
        regex_pattern: str = '',
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Lists files that are inside in a particular stage
        @param stage_name: the location where the data files are staged
        @param regex_pattern: a regular expression pattern for filtering files from the output
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "LIST" \
                    f" {stage_name}" \
                    f"{f' PATTERN = {regex_pattern}' if regex_pattern != '' else ''}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)


class Role:

    def __init__(
        self,
        snowflake_driver: Snowflake
    ):
        self.__snowflake_driver = snowflake_driver

    def use(
        self,
        role_name: str,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Sets particular role for a session
        @param role_name: name of the role to use
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "USE" \
                    f" ROLE {role_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def get_current(
        self,
    ) -> str:
        """
        Returns the name of the role in use by the current session
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "SELECT CURRENT_ROLE()"

        return self.__snowflake_driver.execute(statement, n=1)['results'][0][0]


class StorageIntegration:

    def __init__(
        self,
        snowflake_driver: Snowflake
    ):
        self.__snowflake_driver = snowflake_driver

    def create(
        self,
        storage_integration_name: str,
        storage_provider: str,
        storage_allowed_locations: List[str],
        or_replace: bool = False,
        if_not_exists: bool = False,
        enabled: bool = True,
        silent: bool = False,
        **kwargs
    ) -> Optional[Dict]:
        """
        Executes command to create a Snowflake stage with a given name
        @param storage_integration_name: storage integration name to create
        @param storage_provider: cloud provider to create stage and fetch data from
        @param storage_allowed_locations: explicitly limits external stages
            that use the integration to reference one or more storage locations
        @param or_replace: replace file format if exists
        @param if_not_exists: create object if it doesn't exist so far
        @param enabled: specifies whether this storage integration is available for usage in stages
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        storage_allowed_loccations_string = ",".join([f"'{file}'" for file in storage_allowed_locations])

        statement = "CREATE" \
                    f"{' OR REPLACE' if or_replace else ''}" \
                    f" STORAGE INTEGRATION{' IF NOT EXISTS' if if_not_exists else ''} {storage_integration_name}" \
                    f" TYPE = EXTERNAL_STAGE" \
                    f" ENABLED = {'TRUE' if enabled else 'FALSE'}" \
                    f" STORAGE_PROVIDER = {storage_provider}" \
                    f" STORAGE_ALLOWED_LOCATIONS = ({storage_allowed_loccations_string})"

        # looping through kwargs for extra arguments passed in statement
        # while executing final command, especially cloud-provider-specific parameters,
        # Snowflake will do the validation
        for key, value in kwargs.items():
            statement += f" {key} = {value}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def drop(
        self,
        storage_integration_name: str,
        if_exists: bool = False,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Executes command to drop a Snowflake storage integration with a given name
        @param storage_integration_name: storage integration name to drop
        @param if_exists: adds `IF EXISTS` statement to a command
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "DROP STORAGE INTEGRATION" \
                    f"{' IF EXISTS' if if_exists else ''}" \
                    f" {storage_integration_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)


class Warehouse:

    def __init__(
        self,
        snowflake_driver: Snowflake
    ):
        self.__snowflake_driver = snowflake_driver

    def use(
        self,
        warehouse_name: str,
        silent: bool = False
    ) -> Optional[Dict]:
        """
        Sets particular warehouse for a session
        @param warehouse_name: name of the warehouse to use
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "USE" \
                    f" WAREHOUSE {warehouse_name}"

        return self.__snowflake_driver.execute(statement, n=1, silent=silent)

    def get_current(
        self,
    ) -> str:
        """
        Returns the name of the warehouse in use by the current session
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "SELECT CURRENT_WAREHOUSE()"

        return self.__snowflake_driver.execute(statement, n=1)['results'][0][0]
