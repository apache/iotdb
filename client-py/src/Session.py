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

import sys
import struct

# sys.path.append("../target")
sys.path.append("../../thrift/target/generated-sources-python")
sys.path.append("./utils")

from IoTDBConstants import *
from SessionDataSet import SessionDataSet

from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport import TSocket, TTransport

from iotdb.rpc.TSIService import Client, TSCreateTimeseriesReq, TSInsertRecordReq, TSInsertTabletReq, \
     TSExecuteStatementReq, TSOpenSessionReq, TSQueryDataSet, TSFetchResultsReq, TSCloseOperationReq, \
     TSCreateMultiTimeseriesReq, TSCloseSessionReq, TSInsertTabletsReq, TSInsertRecordsReq
from iotdb.rpc.ttypes import TSDeleteDataReq, TSProtocolVersion, TSSetTimeZoneReq


class Session(object):
    DEFAULT_FETCH_SIZE = 10000
    DEFAULT_USER = 'root'
    DEFAULT_PASSWORD = 'root'

    def __init__(self, host, port, user=DEFAULT_USER, password=DEFAULT_PASSWORD, fetch_size=DEFAULT_FETCH_SIZE):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__fetch_size = fetch_size
        self.__is_close = True
        self.__transport = None
        self.__client = None
        self.protocol_version = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3
        self.__session_id = None
        self.__statement_id = None
        self.__zone_id = None

    def open(self, enable_rpc_compression):
        if not self.__is_close:
            return
        self.__transport = TTransport.TFramedTransport(TSocket.TSocket(self.__host, self.__port))

        if not self.__transport.isOpen():
            try:
                self.__transport.open()
            except TTransport.TTransportException as e:
                print('TTransportException: ', e)

        if enable_rpc_compression:
            self.__client = Client(TCompactProtocol.TCompactProtocol(self.__transport))
        else:
            self.__client = Client(TBinaryProtocol.TBinaryProtocol(self.__transport))

        open_req = TSOpenSessionReq(client_protocol=self.protocol_version,
                                    username=self.__user,
                                    password=self.__password)

        try:
            open_resp = self.__client.openSession(open_req)

            if self.protocol_version != open_resp.serverProtocolVersion:
                print("Protocol differ, Client version is {}, but Server version is {}".format(
                    self.protocol_version, open_resp.serverProtocolVersion))
                # version is less than 0.10
                if open_resp.serverProtocolVersion == 0:
                    raise TTransport.TException(message="Protocol not supported.")

            self.__session_id = open_resp.sessionId
            self.__statement_id = self.__client.requestStatementId(self.__session_id)

        except Exception as e:
            self.__transport.close()
            print("session closed because: ", e)

        if self.__zone_id is not None:
            self.set_time_zone(self.__zone_id)
        else:
            self.__zone_id = self.get_time_zone()

        self.__is_close = False

    def close(self):
        if self.__is_close:
            return
        req = TSCloseSessionReq(self.__session_id)
        try:
            self.__client.closeSession(req)
        except TTransport.TException as e:
            print("Error occurs when closing session at server. Maybe server is down. Error message: ", e)
        finally:
            self.__is_close = True
            if self.__transport is not None:
                self.__transport.close()

    def set_storage_group(self, group_name):
        """
        set one storage group
        :param group_name: String, storage group name (starts from root)
        """
        status = self.__client.setStorageGroup(self.__session_id, group_name)
        print("setting storage group {} message: {}".format(group_name, status.message))

    def delete_storage_group(self, storage_group):
        """
        delete one storage group.
        :param storage_group: String, path of the target storage group.
        """
        groups = [storage_group]
        self.delete_storage_groups(groups)

    def delete_storage_groups(self, storage_group_lst):
        """
        delete multiple storage groups.
        :param storage_group_lst: List, paths of the target storage groups.
        """
        status = self.__client.deleteStorageGroups(self.__session_id, storage_group_lst)
        print("delete storage group(s) {} message: {}".format(storage_group_lst, status.message))

    def create_time_series(self, ts_path, data_type, encoding, compressor):
        """
        create single time series
        :param ts_path: String, complete time series path (starts from root)
        :param data_type: TSDataType, data type for this time series
        :param encoding: TSEncoding, encoding for this time series
        :param compressor: Compressor, compressing type for this time series
        """
        data_type = data_type.value
        encoding = encoding.value
        compressor = compressor.value
        request = TSCreateTimeseriesReq(self.__session_id, ts_path, data_type, encoding, compressor)
        status = self.__client.createTimeseries(request)
        print("creating time series {} message: {}".format(ts_path, status.message))

    def create_multi_time_series(self, ts_path_lst, data_type_lst, encoding_lst, compressor_lst):
        """
        create multiple time series
        :param ts_path_lst: List of String, complete time series paths (starts from root)
        :param data_type_lst: List of TSDataType, data types for time series
        :param encoding_lst: List of TSEncoding, encodings for time series
        :param compressor_lst: List of Compressor, compressing types for time series
        """
        data_type_lst = [data_type.value for data_type in data_type_lst]
        encoding_lst = [encoding.value for encoding in encoding_lst]
        compressor_lst = [compressor.value for compressor in compressor_lst]

        request = TSCreateMultiTimeseriesReq(self.__session_id, ts_path_lst, data_type_lst,
                                             encoding_lst, compressor_lst)
        status = self.__client.createMultiTimeseries(request)
        print("creating multiple time series {} message: {}".format(ts_path_lst, status.message))

    def delete_time_series(self, paths_list):
        """
        delete multiple time series, including data and schema
        :param paths_list: List of time series path, which should be complete (starts from root)
        """
        status = self.__client.deleteTimeseries(self.__session_id, paths_list)
        print("deleting multiple time series {} message: {}".format(paths_list, status.message))

    def check_time_series_exists(self, path):
        """
        check whether a specific time series exists
        :param path: String, complete path of time series for checking
        :return Boolean value indicates whether it exists.
        """
        data_set = self.execute_query_statement("SHOW TIMESERIES {}".format(path))
        result = data_set.has_next()
        data_set.close_operation_handle()
        return result

    def delete_data(self, paths_list, timestamp):
        """
        delete all data <= time in multiple time series
        :param paths_list: time series list that the data in.
        :param timestamp: data with time stamp less than or equal to time will be deleted.
        """
        request = TSDeleteDataReq(self.__session_id, paths_list, timestamp)
        try:
            status = self.__client.deleteData(request)
            print("delete data from {}, message: {}".format(paths_list, status.message))
        except TTransport.TException as e:
            print("data deletion fails because: ", e)

    def insert_str_record(self, device_id, timestamp, measurements, string_values):
        """ special case for inserting one row of String (TEXT) value """
        data_types = [TSDataType.TEXT.value for _ in string_values]
        request = self.gen_insert_record_req(device_id, timestamp, measurements, data_types, string_values)
        status = self.__client.insertRecord(request)
        print("insert one record to device {} message: {}".format(device_id, status.message))

    def insert_record(self, device_id, timestamp, measurements, data_types, values):
        """
        insert one row of record into database, if you want improve your performance, please use insertTablet method
            for example a record at time=10086 with three measurements is:
                timestamp,     m1,    m2,     m3
                    10086,  125.3,  True,  text1
        :param device_id: String, time series path for device
        :param timestamp: Integer, indicate the timestamp of the row of data
        :param measurements: List of String, sensor names
        :param data_types: List of TSDataType, indicate the data type for each sensor
        :param values: List, values to be inserted, for each sensor
        """
        data_types = [data_type.value for data_type in data_types]
        request = self.gen_insert_record_req(device_id, timestamp, measurements, data_types, values)
        status = self.__client.insertRecord(request)
        print("insert one record to device {} message: {}".format(device_id, status.message))

    def insert_records(self, device_ids, times, measurements_lst, types_lst, values_lst):
        """
        insert multiple rows of data, records are independent to each other, in other words, there's no relationship
        between those records
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        """
        type_values_lst = []
        for types in types_lst:
            data_types = [data_type.value for data_type in types]
            type_values_lst.append(data_types)
        request = self.gen_insert_records_req(device_ids, times, measurements_lst, type_values_lst, values_lst)
        status = self.__client.insertRecords(request)
        print("insert multiple records to devices {} message: {}".format(device_ids, status.message))

    def test_insert_record(self, device_id, timestamp, measurements, data_types, values):
        """
        this method NOT insert data into database and the server just return after accept the request, this method
        should be used to test other time cost in client
        :param device_id: String, time series path for device
        :param timestamp: Integer, indicate the timestamp of the row of data
        :param measurements: List of String, sensor names
        :param data_types: List of TSDataType, indicate the data type for each sensor
        :param values: List, values to be inserted, for each sensor
        """
        data_types = [data_type.value for data_type in data_types]
        request = self.gen_insert_record_req(device_id, timestamp, measurements, data_types, values)
        status = self.__client.testInsertRecord(request)
        print("testing! insert one record to device {} message: {}".format(device_id, status.message))

    def test_insert_records(self, device_ids, times, measurements_lst, types_lst, values_lst):
        """
        this method NOT insert data into database and the server just return after accept the request, this method
        should be used to test other time cost in client
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        """
        type_values_lst = []
        for types in types_lst:
            data_types = [data_type.value for data_type in types]
            type_values_lst.append(data_types)
        request = self.gen_insert_records_req(device_ids, times, measurements_lst, type_values_lst, values_lst)
        status = self.__client.testInsertRecords(request)
        print("testing! insert multiple records, message: {}".format(status.message))

    def gen_insert_record_req(self, device_id, timestamp, measurements, data_types, values):
        if (len(values) != len(data_types)) or (len(values) != len(measurements)):
            print("length of data types does not equal to length of values!")
            # could raise an error here.
            return
        values_in_bytes = Session.value_to_bytes(data_types, values)
        return TSInsertRecordReq(self.__session_id, device_id, measurements, values_in_bytes, timestamp)

    def gen_insert_records_req(self, device_ids, times, measurements_lst, types_lst, values_lst):
        if (len(device_ids) != len(measurements_lst)) or (len(times) != len(types_lst)) or \
           (len(device_ids) != len(times)) or (len(times) != len(values_lst)):
            print("deviceIds, times, measurementsList and valuesList's size should be equal")
            # could raise an error here.
            return

        value_lst = []
        for values, data_types, measurements in zip(values_lst, types_lst, measurements_lst):
            if (len(values) != len(data_types)) or (len(values) != len(measurements)):
                print("deviceIds, times, measurementsList and valuesList's size should be equal")
                # could raise an error here.
                return
            values_in_bytes = Session.value_to_bytes(data_types, values)
            value_lst.append(values_in_bytes)

        return TSInsertRecordsReq(self.__session_id, device_ids, measurements_lst, value_lst, times)

    def insert_tablet(self, tablet):
        """
        insert one tablet, in a tablet, for each timestamp, the number of measurements is same
            for example three records in the same device can form a tablet:
                timestamps,     m1,    m2,     m3
                         1,  125.3,  True,  text1
                         2,  111.6, False,  text2
                         3,  688.6,  True,  text3
        Notice: The tablet should not have empty cell
                The tablet itself is sorted (see docs of Tablet.py)
        :param tablet: a tablet specified above
        """
        status = self.__client.insertTablet(self.gen_insert_tablet_req(tablet))
        print("insert one tablet to device {} message: {}".format(tablet.get_device_id(), status.message))

    def insert_tablets(self, tablet_lst):
        """
        insert multiple tablets, tablets are independent to each other
        :param tablet_lst: List of tablets
        """
        status = self.__client.insertTablets(self.gen_insert_tablets_req(tablet_lst))
        print("insert multiple tablets, message: {}".format(status.message))

    def test_insert_tablet(self, tablet):
        """
         this method NOT insert data into database and the server just return after accept the request, this method
         should be used to test other time cost in client
        :param tablet: a tablet of data
        """
        status = self.__client.testInsertTablet(self.gen_insert_tablet_req(tablet))
        print("testing! insert one tablet to device {} message: {}".format(tablet.get_device_id(), status.message))

    def test_insert_tablets(self, tablet_list):
        """
         this method NOT insert data into database and the server just return after accept the request, this method
         should be used to test other time cost in client
        :param tablet_list: List of tablets
        """
        status = self.__client.testInsertTablets(self.gen_insert_tablets_req(tablet_list))
        print("testing! insert multiple tablets, message: {}".format(status.message))

    def gen_insert_tablet_req(self, tablet):
        data_type_values = [data_type.value for data_type in tablet.get_data_types()]
        return TSInsertTabletReq(self.__session_id, tablet.get_device_id(), tablet.get_measurements(),
                                 tablet.get_binary_values(), tablet.get_binary_timestamps(),
                                 data_type_values, tablet.get_row_number())

    def gen_insert_tablets_req(self, tablet_lst):
        device_id_lst = []
        measurements_lst = []
        values_lst = []
        timestamps_lst = []
        type_lst = []
        size_lst = []
        for tablet in tablet_lst:
            data_type_values = [data_type.value for data_type in tablet.get_data_types()]
            device_id_lst.append(tablet.get_device_id())
            measurements_lst.append(tablet.get_measurements())
            values_lst.append(tablet.get_binary_values())
            timestamps_lst.append(tablet.get_binary_timestamps())
            type_lst.append(data_type_values)
            size_lst.append(tablet.get_row_number())
        return TSInsertTabletsReq(self.__session_id, device_id_lst, measurements_lst,
                                  values_lst, timestamps_lst, type_lst, size_lst)

    def execute_query_statement(self, sql):
        """
        execute query sql statement and returns SessionDataSet
        :param sql: String, query sql statement
        :return: SessionDataSet, contains query results and relevant info (see SessionDataSet.py)
        """
        request = TSExecuteStatementReq(self.__session_id, sql, self.__statement_id, self.__fetch_size)
        resp = self.__client.executeQueryStatement(request)
        return SessionDataSet(sql, resp.columns, resp.dataTypeList, resp.columnNameIndexMap, resp.queryId,
                              self.__client, self.__session_id, resp.queryDataSet, resp.ignoreTimeStamp)

    def execute_non_query_statement(self, sql):
        """
        execute non-query sql statement
        :param sql: String, non-query sql statement
        """
        request = TSExecuteStatementReq(self.__session_id, sql, self.__statement_id)
        try:
            resp = self.__client.executeUpdateStatement(request)
            status = resp.status
            print("execute non-query statement {} message: {}".format(sql, status.message))
        except TTransport.TException as e:
            print("execution of non-query statement fails because: ", e)

    @staticmethod
    def value_to_bytes(data_types, values):
        format_str_list = [">"]
        values_tobe_packed = []
        for data_type, value in zip(data_types, values):
            if data_type == TSDataType.BOOLEAN.value:
                format_str_list.append("h")
                format_str_list.append("?")
                values_tobe_packed.append(TSDataType.BOOLEAN.value)
                values_tobe_packed.append(value)
            elif data_type == TSDataType.INT32.value:
                format_str_list.append("h")
                format_str_list.append("i")
                values_tobe_packed.append(TSDataType.INT32.value)
                values_tobe_packed.append(value)
            elif data_type == TSDataType.INT64.value:
                format_str_list.append("h")
                format_str_list.append("q")
                values_tobe_packed.append(TSDataType.INT64.value)
                values_tobe_packed.append(value)
            elif data_type == TSDataType.FLOAT.value:
                format_str_list.append("h")
                format_str_list.append("f")
                values_tobe_packed.append(TSDataType.FLOAT.value)
                values_tobe_packed.append(value)
            elif data_type == TSDataType.DOUBLE.value:
                format_str_list.append("h")
                format_str_list.append("d")
                values_tobe_packed.append(TSDataType.DOUBLE.value)
                values_tobe_packed.append(value)
            elif data_type == TSDataType.TEXT.value:
                value_bytes = bytes(value, 'utf-8')
                format_str_list.append("h")
                format_str_list.append("i")
                format_str_list.append(str(len(value_bytes)))
                format_str_list.append("s")
                values_tobe_packed.append(TSDataType.TEXT.value)
                values_tobe_packed.append(len(value_bytes))
                values_tobe_packed.append(value_bytes)
            else:
                print("Unsupported data type:" + str(data_type))
                # could raise an error here.
                return
        format_str = ''.join(format_str_list)
        return struct.pack(format_str, *values_tobe_packed)

    def get_time_zone(self):
        if self.__zone_id is not None:
            return self.__zone_id
        try:
            resp = self.__client.getTimeZone(self.__session_id)
        except TTransport.TException as e:
            print("Could not get time zone because: ", e)
            raise Exception
        return resp.timeZone

    def set_time_zone(self, zone_id):
        request = TSSetTimeZoneReq(self.__session_id, zone_id)
        try:
            status = self.__client.setTimeZone(request)
            print("setting time zone_id as {}, message: {}".format(zone_id, status.message))
        except TTransport.TException as e:
            print("Could not get time zone because: ", e)
            raise Exception
        self.__zone_id = zone_id
