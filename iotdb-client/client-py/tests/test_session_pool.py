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
from threading import Thread

from iotdb.IoTDBContainer import IoTDBContainer
from iotdb.SessionPool import create_session_pool, PoolConfig


def test_session_pool():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        max_pool_size = 2
        pool_config = PoolConfig(db.get_ip_address(), db.get_port(), "root", "root", [], 1024, "Asia/Shanghai", 3)
        session_pool = create_session_pool(pool_config, max_pool_size, 3000)
        session = session_pool.get_session()
        session.open(False)
        assert session.is_open() is True

        session2 = session_pool.get_session()
        assert session2 is not None

        timeout = False
        try:
            session_pool.get_session()
        except TimeoutError as e:
            timeout = True
            assert str(e) == "Wait to get session timeout in SessionPool, current pool size: " + str(max_pool_size)
        assert timeout is True

        Thread(target=lambda: session_pool.put_back(session2)).start()
        session3 = session_pool.get_session()
        assert session3 is not None

        session_pool.close()
