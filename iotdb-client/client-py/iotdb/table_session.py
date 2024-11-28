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
from typing import Union

from iotdb.Session import Session
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.SessionDataSet import SessionDataSet
from iotdb.utils.Tablet import Tablet


class TableSessionConfig(object):

    def __init__(
        self,
        node_urls: list = None,
        username: str = Session.DEFAULT_USER,
        password: str = Session.DEFAULT_PASSWORD,
        database: str = None,
        fetch_size: int = 5000,
        time_zone: str = Session.DEFAULT_ZONE_ID,
        enable_compression: bool = False,
    ):
        if node_urls is None:
            node_urls = ["localhost:6667"]
        self.node_urls = node_urls
        self.username = username
        self.password = password
        self.database = database
        self.fetch_size = fetch_size
        self.time_zone = time_zone
        self.enable_compression = enable_compression


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
            )
            self.__session.sql_dialect = "table"
            self.__session.database = table_session_config.database
            self.__session.open(table_session_config.enable_compression)
        else:
            self.__session = self.__session_pool.get_session()

    def insert(self, tablet: Union[Tablet, NumpyTablet]):
        self.__session.insert_relational_tablet(tablet)

    def execute_non_query_statement(self, sql: str):
        self.__session.execute_non_query_statement(sql)

    def execute_query_statement(
        self, sql: str, timeout_in_ms: int = 0
    ) -> SessionDataSet:
        return self.__session.execute_query_statement(sql, timeout_in_ms)

    def close(self):
        if self.__session_pool is None:
            self.__session.close()
        else:
            self.__session_pool.put_back(self.__session)
