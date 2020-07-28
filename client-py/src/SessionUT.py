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
        # clean up storage groups and time series.
        self.session.delete_storage_group("root.sg1")
        self.session.close()

    def test_insert_by_str(self):
        self.session.set_storage_group("root.sg1")

        self.createTimeSeries(self.session)
        self.insertByStr(self.session)

        # sql test
        self.insert_via_sql(self.session)
        self.query3()

    def test_insert_by_blank_str_infer_type(self):
        device_id = "root.sg1.d1"
        measurements = ["s1 "]
        values = ["1.0"]
        self.session.insert_str_record(device_id, 1, measurements, values)

        expected = ["root.sg1.d1.s1 "]

        self.assertFalse(self.session.check_time_series_exists("root.sg1.d1.s1 "))
        data_set = self.session.execute_query_statement("show timeseries")
        i = 0
        while data_set.has_next():
            self.assertEquals(expected[i], str(data_set.next().get_fields()[0]))
            i += 1
        self.assertEqual(i, len(expected))

    def test_insert_by_str_infer_type(self):
        device_id = "root.sg1.d1"
        measurements = ["s1", "s2", "s3", "s4"]
        values = ["1", "1.2", "true", "dad"]
        self.session.insert_str_record(device_id, 1, measurements, values)

        expected = [TSDataType.FLOAT.name, TSDataType.FLOAT.name, TSDataType.BOOLEAN.name, TSDataType.TEXT.name]
        data_set = self.session.execute_query_statement("show timeseries root")
        i = 0
        while data_set.has_next():
            self.assertEquals(expected[i], str(data_set.next().get_fields()[3]))
            i += 1
        self.assertEqual(i, len(expected))

    def test_insert_by_obj_not_infer_type(self):
        device_id = "root.sg1.d1"
        measurements = ["s1", "s2", "s3", "s4"]
        data_types = [TSDataType.INT64, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TEXT]
        values = [1, 1.2, "true", "dad"]
        self.session.insert_record(device_id, 1, measurements, data_types, values)

        expected = [TSDataType.INT64.name, TSDataType.DOUBLE.name, TSDataType.TEXT.name, TSDataType.TEXT.name]

        data_set = self.session.execute_query_statement("show timeseries root")
        i = 0
        while data_set.has_next():
            self.assertEquals(expected[i], str(data_set.next().get_fields()[3]))
            i += 1
        self.assertEqual(i, len(expected))

    def test_insert_by_obj(self):
        self.session.set_storage_group("root.sg1")
        self.createTimeSeries(self.session)
        self.insertInObj(self.session)

        # SQL tests
        self.insert_via_sql(self.session)
        self.query3()

    def test_align_by_device(self):
        self.session.set_storage_group("root.sg1")
        self.createTimeSeries(self.session)
        self.insertTabletTest2(self.session, "root.sg1.d1")

        self.queryForAlignByDevice()
        self.queryForAlignByDevice2()

    def test_time(self):
        """ it will output too much trivia, so ignore it. """
        pass

    def test_batch_insert_seq_and_unseq(self):
        self.session.set_storage_group("root.sg1")
        self.createTimeSeries(self.session)
        self.insertTabletTest2("root.sg1.d1")
        self.session.execute_non_query_statement("FLUSH")
        self.session.execute_non_query_statement("FLUSH root.sg1")
        self.session.execute_non_query_statement("MERGE")
        self.session.execute_non_query_statement("FULL MERGE")

        self.insertTabletTest3(self.session, "root.sg1.d1")
        # not yet done! JDBC connection required.
        self.queryForBatchSeqAndUnseq()

    def test_batch_insert(self):
        self.session.set_storage_group("root.sg1")
        self.createTimeSeries(self.session)
        self.insertTabletTest2(self.session, "root.sg1.d1")
        # not yet done! JDBC connection required.
        self.queryForBatch()

    def test_create_multi_time_series(self):
        paths = ["root.sg1.d1.s1", "root.sg1.d1.s2"]
        types = [TSDataType.DOUBLE, TSDataType.DOUBLE]
        encodings = [TSEncoding.RLE, TSEncoding.RLE]
        compressor = [Compressor.SNAPPY, Compressor.SNAPPY]
        self.session.create_multi_time_series(paths, types, encodings, compressor)

        self.assertTrue(self.session.check_time_series_exists("root.sg1.d1.s1"))
        self.assertTrue(self.session.check_time_series_exists("root.sg1.d1.s2"))

    def test_test_method(self):
        self.session.set_storage_group("root.sg1")
        self.createTimeSeries(self.session)
        device_id = "root.sg1.d1"

        # test insert tablet
        measurements = ["s1", "s2", "s3"]
        timestamps = [1]
        values = [[1, 2, 3]]
        types = [TSDataType.INT64, TSDataType.INT64, TSDataType.INT64]
        tablet = Tablet(device_id, measurements, types, values, timestamps)
        self.session.test_insert_tablet(tablet)

        # test insert record
        measurements = ["s1", "s2", "s3"]
        data_types = [TSDataType.INT64, TSDataType.INT64, TSDataType.INT64]
        values = [1, 2, 3]
        for time in range(100):
            self.session.test_insert_record(device_id, time, measurements, data_types, values)

        # test insert records
        measurements = ["s1", "s2", "s3"]
        device_ids = [device_id for _ in range(100)]
        data_types = [[TSDataType.INT64, TSDataType.INT64, TSDataType.INT64] for _ in range(100)]
        values = [[1, 2, 3] for _ in range(100)]
        timestamps = [i for i in range(100)]
        self.session.test_insert_records(device_ids, timestamps, measurements, data_types, values)

    def test_Chinese_character(self):
        storage_group = "root.存储组1"
        devices = ["设备1.指标1", "设备1.s2", "d2.s1", "d2.指标2"]
        self.session.set_storage_group(storage_group)
        self.createTimeSeriesInChinese(self.session, storage_group, devices)
        self.insertInChinese(self.session, storage_group, devices)
        self.session.delete_storage_group(storage_group)

    def test(self):
        self.session.set_storage_group("root.sg1")
        self.createTimeSeries(self.session)
        self.insert(self.session)
        # SQL tests
        self.insert_via_sql(self.session)
        self.query3()
        self.insertTabletTest1(self.session, "root.sg1.d1")
        self.deleteData(self.session)
        self.query()
        self.deleteTimeseries(self.session)
        self.query2()
        self.insertRecords(self.session)
        self.query4()

        # special characters
        self.session.create_time_series("root.sg1.d1.1_2", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.session.create_time_series("root.sg1.d1.\"1.2.3\"", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.session.create_time_series("root.sg1.d1.\'1.2.4\'", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)

        self.session.set_storage_group("root.1")
        self.session.create_time_series("root.1.2.3", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)

        # Add another storage group to test the deletion of storage group
        self.session.set_storage_group("root.sg2")
        self.session.create_time_series("root.sg2.d1.s1", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.deleteStorageGroupTest(self.session)

        # set storage group without creating time series
        self.session.set_storage_group("root.sg3")
        self.insertTabletTest1(self.session, "root.sg3.d1")

        # create timeseries without setting storage groups
        self.session.create_time_series("root.sg4.d1.s1", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.session.create_time_series("root.sg4.d1.s2", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.session.create_time_series("root.sg4.d1.s3", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.insertTabletTest1(self.session, "root.sg4.d1")

        # directly insertion, without any setting of storage groups and time series.
        self.insertTabletTest1(self.session, "root.sg5.d1")

        # clear
        self.session.delete_storage_groups(["root.sg3", "root.sg4", "root.sg5", "root.1"])

    def test_by_str(self):
        self.session.set_storage_group("root.sg1")
        self.createTimeSeries(self.session)
        self.insertByStr(self.session)
        # SQL tests
        self.insert_via_sql(self.session)
        self.query3()
        self.insertTabletTest1(self.session, "root.sg1.d1")
        self.deleteData(self.session)
        self.query()
        self.deleteTimeseries(self.session)
        self.query2()
        self.insertRecordsByStr(self.session)
        self.query4()

        # special characters
        self.session.create_time_series("root.sg1.d1.1_2", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.session.create_time_series("root.sg1.d1.\"1.2.3\"", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.session.create_time_series("root.sg1.d1.\'1.2.4\'", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)

        self.session.set_storage_group("root.1")
        self.session.create_time_series("root.1.2.3", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)

        # Add another storage group to test the deletion of storage group
        self.session.set_storage_group("root.sg2")
        self.session.create_time_series("root.sg2.d1.s1", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.deleteStorageGroupTest(self.session)

        # set storage group without creating time series
        self.session.set_storage_group("root.sg3")
        self.insertTabletTest1(self.session, "root.sg3.d1")

        # create timeseries without setting storage groups
        self.session.create_time_series("root.sg4.d1.s1", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.session.create_time_series("root.sg4.d1.s2", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.session.create_time_series("root.sg4.d1.s3", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        self.insertTabletTest1(self.session, "root.sg4.d1")

        # directly insertion, without any setting of storage groups and time series.
        self.insertTabletTest1(self.session, "root.sg5.d1")

        # clear
        self.session.delete_storage_groups(["root.sg3", "root.sg4", "root.sg5", "root.1"])

    def test_session_interfaces_with_disabled_WAL(self):
        pass

    def query(self):
        """ JDBC connection required. """
        pass

    def query2(self):
        """ JDBC connection required. """
        pass

    def query3(self):
        session_data_set = self.session.execute_query_statement("select * from root.sg1.d1")
        session_data_set.set_fetch_size(1024)
        try:
            count = 0
            while session_data_set.has_next():
                index = 1
                count += 1
                for f in session_data_set.next().get_fields():
                    self.assertEqual(str(index), f.get_string_value())
                    index += 1

            self.assertEqual(101, count)
        finally:
            session_data_set.close_operation_handle()

    def query4(self):
        data_set = self.session.execute_query_statement("select * from root.sg1.d2")
        data_set.set_fetch_size(1024)
        count = 0
        while data_set.has_next():
            index = 1
            count += 1
            for f in data_set.next().get_fields():
                self.assertEqual(index, f.get_long_value())
                index += 1

        self.assertEqual(500, count)
        data_set.close_operation_handle()

    def queryForAlignByDevice(self):
        data_set = self.session.execute_query_statement("select '11', s1, '11' from root.sg1.d1 align by device")
        data_set.set_fetch_size(1024)
        count = 0
        while data_set.has_next():
            count += 1
            string_builder = []
            fields = data_set.next().get_fields()
            for field in fields:
                string_builder.append(field.get_string_value())
                string_builder.append(",")
            self.assertEqual("root.sg1.d1,11,0,11,", "".join(string_builder))

        self.assertEqual(1000, count)
        data_set.close_operation_handle()

    def queryForAlignByDevice2(self):
        data_set = \
            self.session.execute_query_statement("select '11', s1, '11', s5, s1, s5 from root.sg1.d1 align by device")
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

    def queryForBatchSeqAndUnseq(self):
        """ JDBC connection required. """
        pass

    def queryForBatch(self):
        """ JDBC connection required. """
        pass

    @staticmethod
    def createTimeSeries(session):
        session.create_time_series("root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        session.create_time_series("root.sg1.d1.s2", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        session.create_time_series("root.sg1.d1.s3", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        session.create_time_series("root.sg1.d2.s1", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        session.create_time_series("root.sg1.d2.s2", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)
        session.create_time_series("root.sg1.d2.s3", TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)

    @staticmethod
    def createTimeSeriesInChinese(session, storage_group, devices):
        for device in devices:
            full_path = storage_group + '.' + device
            session.create_time_series(full_path, TSDataType.INT64, TSEncoding.RLE, Compressor.SNAPPY)

    @staticmethod
    def deleteTimeseries(session):
        session.delete_time_series(["root.sg1.d1.s1"])

    @staticmethod
    def deleteStorageGroupTest(session):
        try:
            session.delete_storage_group("root.sg1.d1.s1")
        except Exception as e:
            print("The storage group does not exist.")

        sgs = ["root.sg1", "root.sg2"]
        session.delete_storage_groups(sgs)

    @staticmethod
    def deleteData(session):
        paths = ["root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3"]
        end_time = 100
        start_time = 0
        session.delete_data(paths, start_time, end_time)

    @staticmethod
    def insertInChinese(session, storage_group, devices):
        for device in devices:
            for i in range(10):
                ss = device.split('.')
                device_id = storage_group
                for j in range(len(ss) - 1):
                    device_id += '.' + ss[j]
                sensor_id = ss[-1]
                measurement = [sensor_id]
                types = [TSDataType.INT64]
                values = [100]
                session.insert_record(device_id, i, measurement, types, values)

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

    @staticmethod
    def insertInObj(session):
        device_id = "root.sg1.d1"
        measurements = ["s1", "s2", "s3"]
        types = [TSDataType.INT64, TSDataType.INT64, TSDataType.INT64]

        for time in range(100):
            session.insert_record(device_id, time, measurements, types, [1, 2, 3])

    @staticmethod
    def insertRecords(session):
        device_id = "root.sg1.d2"
        device_ids = [device_id for _ in range(500)]
        types = [[TSDataType.INT64, TSDataType.INT64, TSDataType.INT64] for _ in range(500)]
        values = [[1, 2, 3] for _ in range(500)]
        timestamps = [i for i in range(500)]
        measurements = [["s1", "s2", "s3"] for _ in range(500)]

        session.insert_records(device_ids, timestamps, measurements, types, values)

    @staticmethod
    def insertTabletTest1(session, device_id):
        measurements = ["s1", "s2", "s3"]
        timestamps = [i for i in range(100)]
        values = [[0, 1, 2] for _ in range(100)]
        types = [TSDataType.INT64, TSDataType.INT64, TSDataType.INT64]

        tablet = Tablet(device_id, measurements, types, values, timestamps)
        session.insert_tablet(tablet)

    @staticmethod
    def insertTabletTest2(session, device_id):
        measurements = ["s1", "s2", "s3"]
        timestamps = [i for i in range(1000)]
        values = [[0, 1, 2] for _ in range(1000)]
        types = [TSDataType.INT64, TSDataType.INT64, TSDataType.INT64]

        tablet = Tablet(device_id, measurements, types, values, timestamps)
        session.insert_tablet(tablet)

    @staticmethod
    def insertTabletTest3(session, device_id):
        measurements = ["s1", "s2", "s3"]
        timestamps = [i for i in range(500, 1500)]
        values = [[0, 1, 2] for _ in range(500, 1500)]
        types = [TSDataType.INT64, TSDataType.INT64, TSDataType.INT64]

        tablet = Tablet(device_id, measurements, types, values, timestamps)
        session.insert_tablet(tablet)

    @staticmethod
    def insert(session):
        device_id = 'root.sg1.d1'
        measurements = ["s1", "s2", "s3"]
        types = [TSDataType.INT64, TSDataType.INT64, TSDataType.INT64]
        values = [1, 2, 3]
        for time in range(100):
            session.insert_record(device_id, time, measurements, types, values)

    @staticmethod
    def insertRecordsByStr(session):
        measurements = [["s1", "s2", "s3"] for _ in range(500)]
        timestamps = [i for i in range(500)]
        values = [["1", "2", "3"] for _ in range(500)]
        device_ids = ["root.sg1.d2" for _ in range(500)]

        session.insert_str_records(device_ids, timestamps, measurements, values)


if __name__ == '__main__':
    unittest.main()
