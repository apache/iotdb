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

import pytest

from iotdb.dbapi import InterfaceError, NotSupportedError, connect
from tests.integration.iotdb_container import IoTDBContainer


def test_default_table_and_explicit_tree_connections():
    with IoTDBContainer("iotdb:dev") as database:
        host = database.get_container_host_ip()
        port = database.get_exposed_port(6667)

        table_connection = connect(host, port)
        tree_connection = connect(host, port, sql_dialect="tree")
        assert not table_connection.is_close
        assert not tree_connection.is_close

        cursor = table_connection.cursor()
        table_connection.commit()
        with pytest.raises(NotSupportedError):
            table_connection.rollback()
        table_connection.close()
        assert table_connection.is_close
        with pytest.raises(InterfaceError):
            cursor.execute("SHOW DATABASES")
        with pytest.raises(InterfaceError):
            table_connection.cursor()

        tree_connection.close()
