# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from typing import Dict, List, Tuple, Union

from iotdb.Session import Session
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.SessionDataSet import SessionDataSet
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet, ColumnType


class TableSessionConfig(object):

    def __init__(
        self,
        node_urls: list = None,
        username: str = Session.DEFAULT_USER,
        password: str = Session.DEFAULT_PASSWORD,
        database: str = None,
        fetch_size: int = 5000,
        time_zone: str = Session.DEFAULT_ZONE_ID,
        enable_redirection: bool = True,
        enable_compression: bool = False,
        use_ssl: bool = False,
        ca_certs: str = None,
        connection_timeout_in_ms: int = None,
    ):
        """
        Initialize a TableSessionConfig object with the provided parameters.

        Parameters:
            node_urls (list, optional): A list of node URLs for the database connection.
                                        Defaults to ["localhost:6667"].
            username (str, optional): The username for the database connection.
                                      Defaults to "root".
            password (str, optional): The password for the database connection.
                                      Defaults to "TimechoDB@2021".
            database (str, optional): The target database to connect to. Defaults to None.
            fetch_size (int, optional): The number of rows to fetch per query. Defaults to 5000.
            time_zone (str, optional): The default time zone for the session.
                                       Defaults to Session.DEFAULT_ZONE_ID.
            enable_redirection (bool, optional): Whether to enable redirection.
                                                 Defaults to False.
            enable_compression (bool, optional): Whether to enable data compression.
                                                 Defaults to False.

        """
        if node_urls is None:
            node_urls = ["localhost:6667"]
        self.node_urls = node_urls
        self.username = username
        self.password = password
        self.database = database
        self.fetch_size = fetch_size
        self.time_zone = time_zone
        self.enable_redirection = enable_redirection
        self.enable_compression = enable_compression
        self.use_ssl = use_ssl
        self.ca_certs = ca_certs
        self.connection_timeout_in_ms = connection_timeout_in_ms


class TableSession(object):

    def __init__(
        self, table_session_config: TableSessionConfig = None, session_pool=None
    ):
        self.__session_pool = session_pool
        if self.__session_pool is None:
            self.__session = Session.init_from_node_urls(
                table_session_config.node_urls,
                table_session_config.username,
                table_session_config.password,
                table_session_config.fetch_size,
                table_session_config.time_zone,
                table_session_config.enable_redirection,
                table_session_config.use_ssl,
                table_session_config.ca_certs,
                table_session_config.connection_timeout_in_ms,
            )
            self.__session.sql_dialect = "table"
            self.__session.database = table_session_config.database
            self.__session.open(table_session_config.enable_compression)
        else:
            self.__session = self.__session_pool.get_session()

    def insert(self, tablet: Union[Tablet, NumpyTablet]):
        """
        Insert data into the database.

        Parameters:
            tablet (Tablet | NumpyTablet): The tablet containing the data to be inserted.
                                           Accepts either a `Tablet` or `NumpyTablet`.

        Raises:
            IoTDBConnectionException: If there is an issue with the database connection.
        """
        if isinstance(tablet, Tablet):
            split_tablets = _split_tablet_by_device_for_object(tablet)
            if split_tablets is not None:
                for split_tablet in split_tablets:
                    self.__session.insert_relational_tablet(split_tablet)
                return
        self.__session.insert_relational_tablet(tablet)

    def execute_non_query_statement(self, sql: str):
        """
        Execute a non-query SQL statement.

        Parameters:
            sql (str): The SQL statement to execute. Typically used for commands
                       such as INSERT, DELETE, or UPDATE.

        Raises:
            IoTDBConnectionException: If there is an issue with the database connection.
        """
        self.__session.execute_non_query_statement(sql)

    def execute_query_statement(
        self, sql: str, timeout_in_ms: int = 0
    ) -> SessionDataSet:
        """
        Execute a query SQL statement and return the result set.

        Parameters:
            sql (str): The SQL query to execute.
            timeout_in_ms (int, optional): Timeout for the query in milliseconds. Defaults to 0,
                                           which means no timeout.

        Returns:
            SessionDataSet: The result set of the query.

        Raises:
            IoTDBConnectionException: If there is an issue with the database connection.
        """
        return self.__session.execute_query_statement(sql, timeout_in_ms)

    def close(self):
        """
        Close the session and release resources.

        Raises:
            IoTDBConnectionException: If there is an issue closing the connection.
        """
        if self.__session_pool is None:
            self.__session.close()
        else:
            self.__session_pool.put_back(self.__session)


def _split_tablet_by_device_for_object(tablet: Tablet):
    data_types = tablet.get_data_types()

    column_types = tablet.get_column_categories()
    tag_indexes = [i for i, c in enumerate(column_types) if c == ColumnType.TAG]
    if not tag_indexes:
        return None

    values = tablet.get_values()
    timestamps = tablet.get_timestamps()
    if len(values) <= 1:
        return None

    grouped_rows: Dict[Tuple[object, ...], List[int]] = {}
    for row_index, row in enumerate(values):
        key = tuple(row[i] for i in tag_indexes)
        if key not in grouped_rows:
            grouped_rows[key] = []
        grouped_rows[key].append(row_index)

    if len(grouped_rows) <= 1:
        return None

    split_tablets = []
    table_name = tablet.get_insert_target_name()
    column_names = tablet.get_measurements()
    for row_indexes in grouped_rows.values():
        sub_values = [values[i] for i in row_indexes]
        sub_timestamps = [timestamps[i] for i in row_indexes]
        split_tablets.append(
            Tablet(
                table_name,
                column_names,
                data_types,
                sub_values,
                sub_timestamps,
                column_types,
            )
        )
    return split_tablets
