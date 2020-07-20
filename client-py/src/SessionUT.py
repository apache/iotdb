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

import unittest
import sys
import struct

sys.path.append("./utils")

from Session import Session
from Tablet import Tablet
from IoTDBConstants import *
from SessionDataSet import SessionDataSet

from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport import TSocket, TTransport


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.session = Session("127.0.0.1", 6667, "root", "root")
        self.session.open(False)

    def tearDown(self) -> None:
        self.session.close()

    def test_insert_by_str(self):
        self.session.set_storage_group("root.sg1")

        self.__createTimeSeries(self.session)
        self.insertByStr(self.session)

        # sql test
        self.insert_via_sql(self.session)
        self.query3()

    def test_insert_by_blank_str_infer_type(self):
        device_id = "root.sg1.d1"
        measurements = ["s1"]
        values = ["1.0"]
        self.session.insert_str_record(device_id, 1, measurements, values)

        expected = "root.sg1.d1.s1 "

        self.assertFalse(self.session.check_time_series_exists("root.sg1.d1.s1 "))
        data_set = self.session.execute_query_statement("show timeseries")
        i = 0
        while data_set.hasNext():
            self.assertEquals(expected[i], str(data_set.next().get_fields().get(0)))
            i += 1

    @staticmethod
    def __createTimeSeries(session):
        session.create_time_series("root.sg1.d1.s1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY)
        session.create_time_series("root.sg1.d1.s2", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY)
        session.create_time_series("root.sg1.d1.s3", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY)
        session.create_time_series("root.sg1.d2.s1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY)
        session.create_time_series("root.sg1.d2.s2", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY)
        session.create_time_series("root.sg1.d2.s3", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY)

    @staticmethod
    def insertByStr(session):

        device_id = "root.sg1.d1"
        measurements = ["s1", "s2", "s3"]

        for time in range(100):
            values = ["1", "2", "3"]
            session.insert_str_record(device_id, time, measurements, values)

    @staticmethod
    def insert_via_sql(session):
        session.execute_non_query_statement("insert into root.sg1.d1(timestamp, s1, s2, s3) values(100, 1, 2, 3)")

    def query3(self):
        session_data_set = self.session.execute_query_statement("select * from root.sg1.d1")
        session_data_set.set_fetch_size(1024)
        count = 0
        while session_data_set.has_next():
            index = 1
            count += 1
            for f in session_data_set.next().get_fields():
                self.assertEqual(str(index), f.get_string_value())
                index += 1

        self.assertEqual(101, count)
        session_data_set.close_operation_handle()


if __name__ == '__main__':
    unittest.main()
