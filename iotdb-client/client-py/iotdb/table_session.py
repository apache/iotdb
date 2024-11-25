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


class TableSession(object):

    def __init__(self, **kwargs):
        self.__session_pool = kwargs.get("__session_pool", None)
        if self.__session_pool is None:
            __node_urls = kwargs.get("node_urls", ["localhost:6667"])
            __username = kwargs.get("username", Session.DEFAULT_USER)
            __password = kwargs.get("password", Session.DEFAULT_PASSWORD)
            __database = kwargs.get("database", None)
            __query_timeout_in_ms = kwargs.get("query_timeout_in_ms", 60000)
            __fetch_size = kwargs.get("fetch_size", 5000)
            __zone_id = kwargs.get("zone_id", Session.DEFAULT_ZONE_ID)
            self.__session = Session.init_from_node_urls(
                __node_urls,
                __username,
                __password,
                __database,
                __query_timeout_in_ms,
                __fetch_size,
            )
            self.__session.open(kwargs.get("enable_rpc_compression", False))
        else:
            self.__session = self.__session_pool.get_session()

    def insert(self, tablet: Union[Tablet | NumpyTablet]):
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
