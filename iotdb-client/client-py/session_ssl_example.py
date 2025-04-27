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

from iotdb.SessionPool import PoolConfig, SessionPool
from iotdb.Session import Session
from iotdb.table_session import TableSession, TableSessionConfig

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
# Configure SSL enabled
use_ssl = True
# Configure certificate path
ca_certs = "/path/server.crt"


def get_data():
    session = Session(
        ip, port_, username_, password_, use_ssl=use_ssl, ca_certs=ca_certs
    )
    session.open(False)
    result = session.execute_query_statement("select * from root.eg.etth")
    df = result.todf()
    df.rename(columns={"Time": "date"}, inplace=True)
    session.close()
    return df


def get_data2():
    pool_config = PoolConfig(
        host=ip,
        port=port_,
        user_name=username_,
        password=password_,
        fetch_size=1024,
        time_zone="Asia/Shanghai",
        max_retry=3,
        use_ssl=use_ssl,
        ca_certs=ca_certs,
    )
    max_pool_size = 5
    wait_timeout_in_ms = 3000
    session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
    session = session_pool.get_session()
    result = session.execute_query_statement("select * from root.eg.etth")
    df = result.todf()
    df.rename(columns={"Time": "date"}, inplace=True)
    session_pool.put_back(session)
    session_pool.close()


def get_table_data():
    pool_config = TableSessionConfig(
        node_urls=["127.0.0.1:6667"],
        username=username_,
        password=password_,
        fetch_size=1024,
        time_zone="Asia/Shanghai",
        use_ssl=use_ssl,
        ca_certs=ca_certs,
    )
    session = TableSession(pool_config)
    result = session.execute_query_statement("select * from test")
    df = result.todf()
    session.close()


if __name__ == "__main__":
    df = get_data()
