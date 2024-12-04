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
from iotdb.Session import Session
from iotdb.SessionPool import SessionPool, PoolConfig
from iotdb.table_session import TableSession


class TableSessionPoolConfig(object):
    def __init__(
        self,
        node_urls: list = None,
        max_pool_size: int = 5,
        username: str = Session.DEFAULT_USER,
        password: str = Session.DEFAULT_PASSWORD,
        database: str = None,
        fetch_size: int = 5000,
        time_zone: str = Session.DEFAULT_ZONE_ID,
        enable_redirection: bool = False,
        enable_compression: bool = False,
        wait_timeout_in_ms: int = 10000,
        max_retry: int = 3,
    ):
        """
        Initialize a TableSessionPoolConfig object with the provided parameters.

        Parameters:
            node_urls (list, optional): A list of node URLs for the database connection.
                                        Defaults to None.
            max_pool_size (int, optional): The maximum number of sessions in the pool.
                                           Defaults to 5.
            username (str, optional): The username for the database connection.
                                      Defaults to Session.DEFAULT_USER.
            password (str, optional): The password for the database connection.
                                      Defaults to Session.DEFAULT_PASSWORD.
            database (str, optional): The target database to connect to. Defaults to None.
            fetch_size (int, optional): The number of rows to fetch per query. Defaults to 5000.
            time_zone (str, optional): The default time zone for the session pool.
                                       Defaults to Session.DEFAULT_ZONE_ID.
            enable_redirection (bool, optional): Whether to enable redirection.
                                                 Defaults to False.
            enable_compression (bool, optional): Whether to enable data compression.
                                                 Defaults to False.
            wait_timeout_in_ms (int, optional): The maximum time (in milliseconds) to wait for a session
                                                to become available. Defaults to 10000.
            max_retry (int, optional): The maximum number of retry attempts for operations. Defaults to 3.

        """
        if node_urls is None:
            node_urls = ["localhost:6667"]
        self.pool_config = PoolConfig(
            node_urls=node_urls,
            user_name=username,
            password=password,
            fetch_size=fetch_size,
            time_zone=time_zone,
            max_retry=max_retry,
            enable_redirection=enable_redirection,
            enable_compression=enable_compression,
        )
        self.max_pool_size = max_pool_size
        self.wait_timeout_in_ms = wait_timeout_in_ms
        self.database = database


class TableSessionPool(object):

    def __init__(self, table_session_pool_config: TableSessionPoolConfig):
        pool_config = table_session_pool_config.pool_config
        max_pool_size = table_session_pool_config.max_pool_size
        wait_timeout_in_ms = table_session_pool_config.wait_timeout_in_ms
        self.__session_pool = SessionPool(
            pool_config, max_pool_size, wait_timeout_in_ms
        )
        self.__session_pool.sql_dialect = "table"
        self.__session_pool.database = table_session_pool_config.database

    def get_session(self) -> TableSession:
        """
        Retrieve a new TableSession instance.

        Returns:
            TableSession: A new session object configured with the session pool.

        Notes:
            The session is initialized with the underlying session pool for managing
            connections. Ensure proper usage of the session's lifecycle.
        """
        return TableSession(None, session_pool=self.__session_pool)

    def close(self):
        """
        Close the session pool and release all resources.

        This method closes the underlying session pool, ensuring that all
        resources associated with it are properly released.

        Notes:
            After calling this method, the session pool cannot be used to retrieve
            new sessions, and any attempt to do so may raise an exception.
        """
        self.__session_pool.close()
