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


class TableSessionPool(object):

    def __init__(self, **kwargs):
        pool_config = PoolConfig(
            node_urls=kwargs.get("node_urls", ["localhost:6667"]),
            user_name=kwargs.get("username", Session.DEFAULT_USER),
            password=kwargs.get("password", Session.DEFAULT_PASSWORD),
            fetch_size=kwargs.get("fetch_size", 5000),
            time_zone=kwargs.get("zone_id", Session.DEFAULT_ZONE_ID),
            max_retry=3,
        )
        max_pool_size = 5
        wait_timeout_in_ms = 3000
        self.__session_pool = SessionPool(
            pool_config, max_pool_size, wait_timeout_in_ms
        )

    def get_session(self) -> TableSession:
        return TableSession(config={"__session_pool": self.__session_pool})

    def close(self):
        self.__session_pool.close()
