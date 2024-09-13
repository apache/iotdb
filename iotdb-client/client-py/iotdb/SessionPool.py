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
import logging
import multiprocessing
import time
from queue import Queue
from threading import Lock

from iotdb.Session import Session

DEFAULT_MULTIPIE = 5
DEFAULT_FETCH_SIZE = 5000
DEFAULT_MAX_RETRY = 3
DEFAULT_TIME_ZONE = "UTC+8"
SQL_DIALECT = "tree"
logger = logging.getLogger("IoTDB")


class PoolConfig(object):
    def __init__(
        self,
        host: str = None,
        port: str = None,
        user_name: str = None,
        password: str = None,
        node_urls: list = None,
        fetch_size: int = DEFAULT_FETCH_SIZE,
        time_zone: str = DEFAULT_TIME_ZONE,
        max_retry: int = DEFAULT_MAX_RETRY,
        enable_compression: bool = False,
        sql_dialect: str = SQL_DIALECT,
        database: str = None,
    ):
        self.host = host
        self.port = port
        if node_urls is None:
            if host is None or port is None:
                raise ValueError(
                    "(host,port) and node_urls cannot be None at the same time."
                )
            node_urls = []
        self.node_urls = node_urls
        self.user_name = user_name
        self.password = password
        self.fetch_size = fetch_size
        self.time_zone = time_zone
        self.max_retry = max_retry
        self.enable_compression = enable_compression
        self.sql_dialect = sql_dialect
        self.database = database


class SessionPool(object):
    def __init__(
        self, pool_config: PoolConfig, max_pool_size: int, wait_timeout_in_ms: int
    ):
        self.__pool_config = pool_config
        self.__max_pool_size = max_pool_size
        self.__wait_timeout_in_ms = wait_timeout_in_ms / 1000
        self.__pool_size = 0
        self.__queue = Queue(max_pool_size)
        self.__lock = Lock()
        self.__closed = False

    def __construct_session(self) -> Session:
        if len(self.__pool_config.node_urls) > 0:
            session = Session.init_from_node_urls(
                self.__pool_config.node_urls,
                self.__pool_config.user_name,
                self.__pool_config.password,
                self.__pool_config.fetch_size,
                self.__pool_config.time_zone,
                enable_redirection=True,
                sql_dialect=self.__pool_config.sql_dialect,
                database=self.__pool_config.database,
            )

        else:
            session = Session(
                self.__pool_config.host,
                self.__pool_config.port,
                self.__pool_config.user_name,
                self.__pool_config.password,
                self.__pool_config.fetch_size,
                self.__pool_config.time_zone,
                enable_redirection=True,
                sql_dialect=self.__pool_config.sql_dialect,
                database=self.__pool_config.database,
            )

        session.open(self.__pool_config.enable_compression)
        return session

    def __poll_session(self):
        q = None
        if not self.__queue.empty():
            q = self.__queue.get(block=False)
        return q

    def get_session(self) -> Session:

        if self.__closed:
            raise ConnectionError("SessionPool has already been closed.")

        should_create = False
        start = time.time()

        session = self.__poll_session()
        while session is None:
            with self.__lock:
                if self.__pool_size < self.__max_pool_size:
                    self.__pool_size += 1
                    should_create = True
                    break
                else:
                    if time.time() - start > self.__wait_timeout_in_ms:
                        raise TimeoutError(
                            "Wait to get session timeout in SessionPool, current pool size: {0}".format(
                                self.__max_pool_size
                            )
                        )
                    time.sleep(1)
            session = self.__poll_session()

        if should_create:
            try:
                session = self.__construct_session()
            except Exception as e:
                with self.__lock:
                    self.__pool_size -= 1
                raise e

        return session

    def put_back(self, session: Session):

        if self.__closed:
            raise ConnectionError(
                "SessionPool has already been closed, please close the session manually."
            )

        if session.is_open():
            self.__queue.put(session)
        else:
            with self.__lock:
                self.__pool_size -= 1

    def close(self):

        with self.__lock:
            while not self.__queue.empty():
                session = self.__queue.get(block=False)
                session.close()
                self.__pool_size -= 1
        self.__closed = True
        logger.info("SessionPool has been closed successfully.")


def create_session_pool(
    pool_config: PoolConfig, max_pool_size: int, wait_timeout_in_ms: int
) -> SessionPool:
    if max_pool_size <= 0:
        max_pool_size = multiprocessing.cpu_count() * DEFAULT_MULTIPIE
    return SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
