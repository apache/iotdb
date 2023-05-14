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
from iotdb.IoTDBContainer import IoTDBContainer

from numpy.testing import assert_array_equal


def test_simple_query():
    with IoTDBContainer("apache/iotdb:0.11.2") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)

        # Write data
        session.insert_str_record("root.device", 123, "pressure", "15.0")

        # Read
        session_data_set = session.execute_query_statement("SELECT * FROM root.*")
        df = session_data_set.todf()

        session.close()

    assert list(df.columns) == ["Time", "root.device.pressure"]
    assert_array_equal(df.values, [[123.0, 15.0]])


def test_non_time_query():
    with IoTDBContainer("apache/iotdb:0.11.2") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)

        # Write data
        session.insert_str_record("root.device", 123, "pressure", "15.0")

        # Read
        session_data_set = session.execute_query_statement("SHOW TIMESERIES")
        df = session_data_set.todf()

        session.close()

    assert list(df.columns) == [
        "timeseries",
        "alias",
        "storage group",
        "dataType",
        "encoding",
        "compression",
        "tags",
        "attributes",
    ]
    assert_array_equal(
        df.values,
        [
            [
                "root.device.pressure",
                None,
                "root.device",
                "FLOAT",
                "GORILLA",
                "SNAPPY",
                None,
                None,
            ]
        ],
    )
