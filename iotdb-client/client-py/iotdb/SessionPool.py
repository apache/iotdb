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
import multiprocessing
import time
from queue import Queue
from threading import Lock

from iotdb.Session import Session


class PoolConfig:
    def __init__(self, host: str, ip: str, node_urls: [], user_name: str, password: str, fetch_size: int,
                 time_zone: str, max_retry: int):
        self.host = host
        self.ip = ip
        self.node_urls = node_urls
        self.user_name = user_name
        self.password = password
        self.fetch_size = fetch_size
        self.time_zone = time_zone
        self.max_retry = max_retry


class SessionPool(object):
    DEFAULT_MULTIPIE = 5

    def __init__(self, pool_config: PoolConfig, max_pool_size: int, wait_timeout_in_ms: int, enable_compression: bool):
        self.__pool_config = pool_config
        self.__max_pool_size = max_pool_size
        self.__wait_timeout_in_ms = wait_timeout_in_ms
        self.__enable_compression = enable_compression
        self.__pool_size = 0
        self.__queue = Queue(max_pool_size)
        self.__lock = Lock()

    def __construct_session(self) -> Session:
        if len(self.__pool_config.node_urls) > 0:
            session = Session.init_from_node_urls(self.__pool_config.node_urls, self.__pool_config.user_name,
                                                  self.__pool_config.password, self.__pool_config.fetch_size,
                                                  self.__pool_config.time_zone)

        else:
            session = Session(self.__pool_config.host, self.__pool_config.ip, self.__pool_config.user_name,
                              self.__pool_config.password, self.__pool_config.fetch_size, self.__pool_config.time_zone)

        try:
            session.open(self.__enable_compression)
            return session
        except Exception as e:
            session.close()
            raise e

    def __poll_session(self) -> Session | None:
        if self.__queue.empty():
            return None
        return self.__queue.get(block=False)

    def get_session(self) -> Session:
        should_create = False
        start = time.time()

        session = self.__poll_session()
        while session is not None:
            self.__lock.acquire()
            if self.__pool_size < self.__max_pool_size:
                self.__pool_size += 1
                should_create = True
                self.__lock.release()
                break
            else:
                if time.time() - start > self.__wait_timeout_in_ms:
                    raise TimeoutError("Wait for session timeout")
                time.sleep(1)
            session = self.__poll_session()
            self.__lock.release()

        if should_create:
            try:
                session = self.__construct_session()
            except Exception as e:
                self.__lock.acquire()
                self.__pool_size -= 1
                self.__lock.release()
                raise e

        return session

    def put_back(self, session: Session):
        if session.is_open():
            self.__queue.put(session)
        else:
            self.__lock.acquire()
            self.__pool_size -= 1
            self.__lock.release()

    def close(self):
        while not self.__queue.empty():
            session = self.__queue.get(block=False)
            session.close()


def new_session_pool(pool_config: PoolConfig, max_pool_size: int, wait_timeout_in_ms: int,
                     enable_compression: bool) -> SessionPool:
    if max_pool_size <= 0:
        max_pool_size = multiprocessing.cpu_count() * SessionPool.DEFAULT_MULTIPIE
    return SessionPool(pool_config, max_pool_size, wait_timeout_in_ms, enable_compression)
