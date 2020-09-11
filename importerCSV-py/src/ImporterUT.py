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

from Session import Session
from importer import Importer
from IoTDBConstants import *
from SessionDataSet import SessionDataSet

from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport import TSocket, TTransport


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.session = Session("127.0.0.1", 6667, "root", "root")
        self.importer = Importer(self.session)

    def test_four_data_types_series(self):
        """int, float, bool, text"""
        self.importer.align_all_series("test1.csv")
        self.query()
        self.query2()

    def test_four_data_types_device(self):
        self.queryForAlignByDevice()
        self.queryForAlignByDevice2()

    def test_time_format(self):
        self.assertEqual(True, False)

    def test_empty_value(self):
        self.assertEqual(True, False)

    def test_null_value(self):
        self.assertEqual(True, False)

    def test_sg_once_series(self):
        self.assertEqual(True, False)

    def test_sg_once_device(self):
        self.assertEqual(True, False)

    def test_incorrect_csv_format(self):
        self.assertEqual(True, False)

    def test_long_type_timestamp(self):
        self.assertEqual(True, False)

    def query(self):
        session_data_set = self.session.execute_query_statement("select * from root.sg1.d1")
        session_data_set.set_fetch_size(1024)
        try:
            count = 0
            lst = []
            while session_data_set.has_next():
                count += 1
                for f in session_data_set.next().get_fields():
                    lst.append(f.get_string_value())
            expected = []
            self.assertEqual(expected, lst)
            self.assertEqual(2, count)
        finally:
            session_data_set.close_operation_handle()

    def query2(self):
        session_data_set = self.session.execute_query_statement(
            "select * from root.sg1.d2")
        session_data_set.set_fetch_size(1024)
        try:
            count = 0
            lst = []
            while session_data_set.has_next():
                count += 1
                for f in session_data_set.next().get_fields():
                    lst.append(f.get_string_value())
            expected = []
            self.assertEqual(expected, lst)
            self.assertEqual(2, count)
        finally:
            session_data_set.close_operation_handle()

    def queryForAlignByDevice(self):
        data_set = \
            self.session.execute_query_statement("select '11', s1, '11', s5, "
                                                 "s1, s5 from root.sg1.d1 "
                                                 "align by device")
        data_set.set_fetch_size(1024)
        count = 0
        while data_set.has_next():
            count += 1
            string_builder = []
            fields = data_set.next().get_fields()
            for field in fields:
                string_builder.append(field.get_string_value())
                string_builder.append(",")
            self.assertEqual("root.sg1.d1,11,0,11,None,0,None,", "".join(string_builder))

        self.assertEqual(1000, count)
        data_set.close_operation_handle()

    def queryForAlignByDevice2(self):
        data_set = \
            self.session.execute_query_statement("select '11', s1, '11', s5, s1, s5 from root.sg1.d2 align by device")
        data_set.set_fetch_size(1024)
        count = 0
        while data_set.has_next():
            count += 1
            string_builder = []
            fields = data_set.next().get_fields()
            for field in fields:
                string_builder.append(field.get_string_value())
                string_builder.append(",")
            self.assertEqual("root.sg1.d1,11,0,11,None,0,None,", "".join(string_builder))

        self.assertEqual(1000, count)
        data_set.close_operation_handle()


if __name__ == '__main__':
    unittest.main()
