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
import random
import struct
import time
import warnings
from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport import TSocket, TTransport

from iotdb.utils.SessionDataSet import SessionDataSet
from .template.Template import Template
from .template.TemplateQueryType import TemplateQueryType
from .thrift.common.ttypes import TEndPoint, TSStatus
from .thrift.rpc.IClientRPCService import (
    Client,
    TSCreateTimeseriesReq,
    TSCreateAlignedTimeseriesReq,
    TSInsertRecordReq,
    TSInsertStringRecordReq,
    TSInsertTabletReq,
    TSExecuteStatementReq,
    TSOpenSessionReq,
    TSCreateMultiTimeseriesReq,
    TSCloseSessionReq,
    TSInsertTabletsReq,
    TSInsertRecordsReq,
    TSInsertRecordsOfOneDeviceReq,
    TSCreateSchemaTemplateReq,
    TSDropSchemaTemplateReq,
    TSAppendSchemaTemplateReq,
    TSPruneSchemaTemplateReq,
    TSSetSchemaTemplateReq,
    TSUnsetSchemaTemplateReq,
    TSQueryTemplateReq,
)
from .thrift.rpc.ttypes import (
    TSDeleteDataReq,
    TSProtocolVersion,
    TSSetTimeZoneReq,
    TSRawDataQueryReq,
    TSLastDataQueryReq,
    TSInsertStringRecordsOfOneDeviceReq,
)
from .tsfile.utils.DateUtils import parse_date_to_int
from .utils.IoTDBConnectionException import IoTDBConnectionException

logger = logging.getLogger("IoTDB")
warnings.simplefilter("always", DeprecationWarning)


class Session(object):
    SUCCESS_STATUS = 200
    MULTIPLE_ERROR = 302
    REDIRECTION_RECOMMEND = 400
    DEFAULT_FETCH_SIZE = 5000
    DEFAULT_USER = "root"
    DEFAULT_PASSWORD = "root"
    DEFAULT_ZONE_ID = time.strftime("%z")
    RETRY_NUM = 3
    SQL_DIALECT = "tree"

    def __init__(
        self,
        host,
        port,
        user=DEFAULT_USER,
        password=DEFAULT_PASSWORD,
        fetch_size=DEFAULT_FETCH_SIZE,
        zone_id=DEFAULT_ZONE_ID,
        enable_redirection=True,
    ):
        self.__host = host
        self.__port = port
        self.__hosts = None
        self.__ports = None
        self.__default_endpoint = TEndPoint(self.__host, self.__port)
        self.__user = user
        self.__password = password
        self.__fetch_size = fetch_size
        self.__is_close = True
        self.__client = None
        self.__default_connection = None
        self.protocol_version = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3
        self.__session_id = None
        self.__statement_id = None
        self.__zone_id = zone_id
        self.__enable_rpc_compression = None
        self.__enable_redirection = enable_redirection
        self.__device_id_to_endpoint = None
        self.__endpoint_to_connection = None
        self.sql_dialect = self.SQL_DIALECT
        self.database = None

    @classmethod
    def init_from_node_urls(
        cls,
        node_urls,
        user=DEFAULT_USER,
        password=DEFAULT_PASSWORD,
        fetch_size=DEFAULT_FETCH_SIZE,
        zone_id=DEFAULT_ZONE_ID,
        enable_redirection=True,
    ):
        if node_urls is None:
            raise RuntimeError("node urls is empty")
        session = Session(
            None,
            None,
            user,
            password,
            fetch_size,
            zone_id,
            enable_redirection,
        )
        session.__hosts = []
        session.__ports = []
        for node_url in node_urls:
            split = node_url.split(":")
            session.__hosts.append(split[0])
            session.__ports.append(int(split[1]))
        session.__host = session.__hosts[0]
        session.__port = session.__ports[0]
        session.__default_endpoint = TEndPoint(session.__host, session.__port)
        return session

    def open(self, enable_rpc_compression=False):
        if not self.__is_close:
            return
        self.__enable_rpc_compression = enable_rpc_compression
        if self.__hosts is None:
            self.__default_connection = self.init_connection(self.__default_endpoint)
        else:
            for i in range(0, len(self.__hosts)):
                self.__default_endpoint = TEndPoint(self.__hosts[i], self.__ports[i])
                try:
                    self.__default_connection = self.init_connection(
                        self.__default_endpoint
                    )
                except Exception as e:
                    if not self.reconnect():
                        if str(e).startswith("Could not connect to any of"):
                            error_msg = (
                                "Cluster has no nodes to connect because: "
                                + self.connection_error_msg()
                            )
                        else:
                            error_msg = str(e)
                        raise IoTDBConnectionException(error_msg) from None
                break
        self.__client = self.__default_connection.client
        self.__session_id = self.__default_connection.session_id
        self.__statement_id = self.__default_connection.statement_id
        self.__is_close = False
        if self.__enable_redirection:
            self.__device_id_to_endpoint = {}
            self.__endpoint_to_connection = {
                str(self.__default_endpoint): self.__default_connection
            }

    def init_connection(self, endpoint):
        transport = TTransport.TFramedTransport(
            TSocket.TSocket(endpoint.ip, endpoint.port)
        )

        if not transport.isOpen():
            try:
                transport.open()
            except TTransport.TTransportException as e:
                raise IoTDBConnectionException(e) from None

        if self.__enable_rpc_compression:
            client = Client(TCompactProtocol.TCompactProtocolAccelerated(transport))
        else:
            client = Client(TBinaryProtocol.TBinaryProtocolAccelerated(transport))

        configuration = {"version": "V_1_0", "sql_dialect": self.sql_dialect}
        if self.database is not None:
            configuration["db"] = self.database
        open_req = TSOpenSessionReq(
            client_protocol=self.protocol_version,
            username=self.__user,
            password=self.__password,
            zoneId=self.__zone_id,
            configuration=configuration,
        )

        try:
            open_resp = client.openSession(open_req)
            Session.verify_success(open_resp.status)

            if self.protocol_version != open_resp.serverProtocolVersion:
                logger.exception(
                    "Protocol differ, Client version is {}, but Server version is {}".format(
                        self.protocol_version, open_resp.serverProtocolVersion
                    )
                )
                # version is less than 0.10
                if open_resp.serverProtocolVersion == 0:
                    raise TTransport.TException(message="Protocol not supported.")

            session_id = open_resp.sessionId
            statement_id = client.requestStatementId(session_id)

        except Exception as e:
            transport.close()
            raise IoTDBConnectionException(e) from None

        if self.__zone_id is not None:
            request = TSSetTimeZoneReq(session_id, self.__zone_id)
            try:
                client.setTimeZone(request)
            except TTransport.TException as e:
                raise IoTDBConnectionException(
                    "Could not set time zone because: ", e
                ) from None
        else:
            self.__zone_id = self.get_time_zone()
        return SessionConnection(client, transport, session_id, statement_id)

    def is_open(self):
        return not self.__is_close

    def close(self):
        if self.__is_close:
            return
        try:
            if self.__enable_redirection:
                for connection in self.__endpoint_to_connection.values():
                    req = TSCloseSessionReq(connection.session_id)
                    connection.close_connection(req)
            else:
                req = TSCloseSessionReq(self.__session_id)
                self.__default_connection.close_connection(req)
        finally:
            self.__is_close = True

    def set_storage_group(self, group_name):
        """
        create one database
        :param group_name: String, database name (starts from root)
        """
        try:
            return Session.verify_success(
                self.__client.setStorageGroup(self.__session_id, group_name)
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.setStorageGroup(self.__session_id, group_name)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def delete_storage_group(self, storage_group):
        """
        delete one database.
        :param storage_group: String, path of the target database.
        """
        groups = [storage_group]
        return self.delete_storage_groups(groups)

    def delete_storage_groups(self, storage_group_lst):
        """
        delete multiple databases.
        :param storage_group_lst: List, paths of the target databases.
        """
        try:
            return Session.verify_success(
                self.__client.deleteStorageGroups(self.__session_id, storage_group_lst)
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.deleteStorageGroups(
                            self.__session_id, storage_group_lst
                        )
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def create_time_series(
        self,
        ts_path,
        data_type,
        encoding,
        compressor,
        props=None,
        tags=None,
        attributes=None,
        alias=None,
    ):
        """
        create single time series
        :param ts_path: String, complete time series path (starts from root)
        :param data_type: TSDataType, data type for this time series
        :param encoding: TSEncoding, encoding for this time series
        :param compressor: Compressor, compressing type for this time series
        :param props: Dictionary, properties for time series
        :param tags: Dictionary, tag map for time series
        :param attributes: Dictionary, attribute map for time series
        :param alias: String, measurement alias for time series
        """
        request = TSCreateTimeseriesReq(
            self.__session_id,
            ts_path,
            data_type,
            encoding,
            compressor,
            props,
            tags,
            attributes,
            alias,
        )
        try:
            return Session.verify_success(self.__client.createTimeseries(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.createTimeseries(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def create_aligned_time_series(
        self, device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
    ):
        """
        create aligned time series
        :param device_id: String, device id for timeseries (starts from root)
        :param measurements_lst: List of String, measurement ids for time series
        :param data_type_lst: List of TSDataType, data types for time series
        :param encoding_lst: List of TSEncoding, encodings for time series
        :param compressor_lst: List of Compressor, compressing types for time series
        """

        request = TSCreateAlignedTimeseriesReq(
            self.__session_id,
            device_id,
            measurements_lst,
            data_type_lst,
            encoding_lst,
            compressor_lst,
        )
        try:
            return Session.verify_success(
                self.__client.createAlignedTimeseries(request)
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.createAlignedTimeseries(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def create_multi_time_series(
        self,
        ts_path_lst,
        data_type_lst,
        encoding_lst,
        compressor_lst,
        props_lst=None,
        tags_lst=None,
        attributes_lst=None,
        alias_lst=None,
    ):
        """
        create multiple time series
        :param ts_path_lst: List of String, complete time series paths (starts from root)
        :param data_type_lst: List of TSDataType, data types for time series
        :param encoding_lst: List of TSEncoding, encodings for time series
        :param compressor_lst: List of Compressor, compressing types for time series
        :param props_lst: List of Props Dictionary, properties for time series
        :param tags_lst: List of tag Dictionary, tag maps for time series
        :param attributes_lst: List of attribute Dictionary, attribute maps for time series
        :param alias_lst: List of alias, measurement alias for time series
        """

        request = TSCreateMultiTimeseriesReq(
            self.__session_id,
            ts_path_lst,
            data_type_lst,
            encoding_lst,
            compressor_lst,
            props_lst,
            tags_lst,
            attributes_lst,
            alias_lst,
        )
        try:
            return Session.verify_success(self.__client.createMultiTimeseries(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.createMultiTimeseries(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def delete_time_series(self, paths_list):
        """
        delete multiple time series, including data and schema
        :param paths_list: List of time series path, which should be complete (starts from root)
        """
        try:
            return Session.verify_success(
                self.__client.deleteTimeseries(self.__session_id, paths_list)
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.deleteTimeseries(self.__session_id, paths_list)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

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

    def delete_data(self, paths_list, end_time):
        """
        delete all data <= end_time in multiple time series
        :param paths_list: time series list that the data in.
        :param end_time: data with time stamp less than or equal to time will be deleted.
        """
        request = TSDeleteDataReq(
            self.__session_id, paths_list, -9223372036854775808, end_time
        )
        try:
            return Session.verify_success(self.__client.deleteData(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.deleteData(request))
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def delete_data_in_range(self, paths_list, start_time, end_time):
        """
        delete data >= start_time and data <= end_time in multiple timeseries
        :param paths_list: time series list that the data in.
        :param start_time: delete range start time.
        :param end_time: delete range end time.
        """
        request = TSDeleteDataReq(self.__session_id, paths_list, start_time, end_time)
        try:
            return Session.verify_success(self.__client.deleteData(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.deleteData(request))
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def insert_str_record(self, device_id, timestamp, measurements, string_values):
        """special case for inserting one row of String (TEXT) value"""
        if type(string_values) == str:
            string_values = [string_values]
        if type(measurements) == str:
            measurements = [measurements]
        if self.__has_none_value(string_values):
            filtered_measurements, filtered_values = zip(
                *[(m, v) for m, v in zip(measurements, string_values) if v is not None]
            )
            measurements = list(filtered_measurements)
            values = list(filtered_values)
            if len(measurements) is 0 or len(values) is 0:
                logger.info("All inserting values are none!")
                return
        request = self.gen_insert_str_record_req(
            device_id, timestamp, measurements, string_values
        )
        try:
            connection = self.get_connection(device_id)
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertStringRecord(request)
            )
        except RedirectException as e:
            return self.handle_redirection(device_id, e.redirect_node)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.insertStringRecord(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def insert_aligned_str_record(
        self, device_id, timestamp, measurements, string_values
    ):
        """special case for inserting one row of String (TEXT) value"""
        if type(string_values) == str:
            string_values = [string_values]
        if type(measurements) == str:
            measurements = [measurements]
        if self.__has_none_value(string_values):
            filtered_measurements, filtered_values = zip(
                *[(m, v) for m, v in zip(measurements, string_values) if v is not None]
            )
            measurements = list(filtered_measurements)
            values = list(filtered_values)
            if len(measurements) is 0 or len(values) is 0:
                logger.info("All inserting values are none!")
                return
        request = self.gen_insert_str_record_req(
            device_id, timestamp, measurements, string_values, True
        )
        try:
            connection = self.get_connection(device_id)
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertStringRecord(request)
            )
        except RedirectException as e:
            return self.handle_redirection(device_id, e.redirect_node)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.insertStringRecord(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

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
        if self.__has_none_value(values):
            filtered_measurements, filtered_data_types, filtered_values = zip(
                *[
                    (m, d, v)
                    for m, d, v in zip(measurements, data_types, values)
                    if v is not None
                ]
            )
            measurements = list(filtered_measurements)
            data_types = list(filtered_data_types)
            values = list(filtered_values)
            if len(measurements) is 0 or len(data_types) is 0 or len(values) is 0:
                logger.info("All inserting values are none!")
                return
        request = self.gen_insert_record_req(
            device_id, timestamp, measurements, data_types, values
        )
        try:
            connection = self.get_connection(device_id)
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertRecord(request)
            )
        except RedirectException as e:
            return self.handle_redirection(device_id, e.redirect_node)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertRecord(request))
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def insert_records(
        self, device_ids: list, times, measurements_lst, types_lst, values_lst
    ):
        """
        insert multiple rows of data, records are independent to each other, in other words, there's no relationship
        between those records
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        """
        if self.__has_none_value(values_lst):
            (
                device_ids,
                times,
                measurements_lst,
                types_lst,
                values_lst,
            ) = self.__filter_lists_by_values(
                device_ids, times, measurements_lst, types_lst, values_lst
            )
            if len(device_ids) is 0:
                logger.info("All inserting values are none!")
                return
        if self.__enable_redirection:
            request_group = {}
            for i in range(len(device_ids)):
                connection = self.get_connection(device_ids[i])
                request = request_group.setdefault(
                    connection.client,
                    TSInsertRecordsReq(connection.session_id, [], [], [], []),
                )
                request.prefixPaths.append(device_ids[i])
                request.timestamps.append(times[i])
                request.measurementsList.append(measurements_lst[i])
                request.valuesList.append(
                    Session.value_to_bytes(types_lst[i], values_lst[i])
                )
            for client, request in request_group.items():
                try:
                    Session.verify_success_with_redirection_for_multi_devices(
                        client.insertRecords(request), request.prefixPaths
                    )
                except RedirectException as e:
                    for device, endpoint in e.device_to_endpoint.items():
                        self.handle_redirection(device, endpoint)
                except TTransport.TException as e:
                    if self.reconnect():
                        try:
                            request.sessionId = self.__session_id
                            Session.verify_success(self.__client.insertRecords(request))
                        except TTransport.TException as e1:
                            raise IoTDBConnectionException(e1) from None
                    else:
                        raise IoTDBConnectionException(
                            self.connection_error_msg()
                        ) from None

            return 0
        else:
            request = self.gen_insert_records_req(
                device_ids, times, measurements_lst, types_lst, values_lst
            )
            try:
                return Session.verify_success(self.__client.insertRecords(request))
            except TTransport.TException as e:
                if self.reconnect():
                    try:
                        request.sessionId = self.__session_id
                        return Session.verify_success(
                            self.__client.insertRecords(request)
                        )
                    except TTransport.TException as e1:
                        raise IoTDBConnectionException(e1) from None
                else:
                    raise IoTDBConnectionException(
                        self.connection_error_msg()
                    ) from None

    def insert_aligned_record(
        self, device_id, timestamp, measurements, data_types, values
    ):
        """
        insert one row of aligned record into database, if you want improve your performance, please use insertTablet method
            for example a record at time=10086 with three measurements is:
                timestamp,     m1,    m2,     m3
                    10086,  125.3,  True,  text1
        :param device_id: String, time series path for device
        :param timestamp: Integer, indicate the timestamp of the row of data
        :param measurements: List of String, sensor names
        :param data_types: List of TSDataType, indicate the data type for each sensor
        :param values: List, values to be inserted, for each sensor
        """
        if self.__has_none_value(values):
            filtered_measurements, filtered_data_types, filtered_values = zip(
                *[
                    (m, d, v)
                    for m, d, v in zip(measurements, data_types, values)
                    if v is not None
                ]
            )
            measurements = list(filtered_measurements)
            data_types = list(filtered_data_types)
            values = list(filtered_values)
            if len(measurements) is 0 or len(data_types) is 0 or len(values) is 0:
                logger.info("All inserting values are none!")
                return
        request = self.gen_insert_record_req(
            device_id, timestamp, measurements, data_types, values, True
        )
        try:
            connection = self.get_connection(device_id)
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertRecord(request)
            )
        except RedirectException as e:
            return self.handle_redirection(device_id, e.redirect_node)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertRecord(request))
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def insert_aligned_records(
        self, device_ids, times, measurements_lst, types_lst, values_lst
    ):
        """
        insert multiple aligned rows of data, records are independent to each other, in other words, there's no relationship
        between those records
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        """
        if self.__has_none_value(values_lst):
            (
                device_ids,
                times,
                measurements_lst,
                types_lst,
                values_lst,
            ) = self.__filter_lists_by_values(
                device_ids, times, measurements_lst, types_lst, values_lst
            )
            if len(device_ids) is 0:
                logger.info("All inserting values are none!")
                return
        if self.__enable_redirection:
            request_group = {}
            for i in range(len(device_ids)):
                connection = self.get_connection(device_ids[i])
                request = request_group.setdefault(
                    connection.client,
                    TSInsertRecordsReq(connection.session_id, [], [], [], [], True),
                )
                request.prefixPaths.append(device_ids[i])
                request.timestamps.append(times[i])
                request.measurementsList.append(measurements_lst[i])
                request.valuesList.append(
                    Session.value_to_bytes(types_lst[i], values_lst[i])
                )
            for client, request in request_group.items():
                try:
                    Session.verify_success_with_redirection_for_multi_devices(
                        client.insertRecords(request), request.prefixPaths
                    )
                except RedirectException as e:
                    for device, endpoint in e.device_to_endpoint.items():
                        self.handle_redirection(device, endpoint)
                except TTransport.TException as e:
                    if self.reconnect():
                        try:
                            request.sessionId = self.__session_id
                            Session.verify_success(self.__client.insertRecords(request))
                        except TTransport.TException as e1:
                            raise IoTDBConnectionException(e1) from None
                    else:
                        raise IoTDBConnectionException(
                            self.connection_error_msg()
                        ) from None

            return 0
        else:
            request = self.gen_insert_records_req(
                device_ids, times, measurements_lst, types_lst, values_lst, True
            )
            try:
                return Session.verify_success(self.__client.insertRecords(request))
            except TTransport.TException as e:
                if self.reconnect():
                    try:
                        request.sessionId = self.__session_id
                        return Session.verify_success(
                            self.__client.insertRecords(request)
                        )
                    except TTransport.TException as e1:
                        raise IoTDBConnectionException(e1) from None
                else:
                    raise IoTDBConnectionException(
                        self.connection_error_msg()
                    ) from None

    def test_insert_record(
        self, device_id, timestamp, measurements, data_types, values
    ):
        """
        this method NOT insert data into database and the server just return after accept the request, this method
        should be used to test other time cost in client
        :param device_id: String, time series path for device
        :param timestamp: Integer, indicate the timestamp of the row of data
        :param measurements: List of String, sensor names
        :param data_types: List of TSDataType, indicate the data type for each sensor
        :param values: List, values to be inserted, for each sensor
        """
        request = self.gen_insert_record_req(
            device_id, timestamp, measurements, data_types, values
        )
        try:
            return Session.verify_success(self.__client.testInsertRecord(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.testInsertRecord(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def test_insert_records(
        self, device_ids, times, measurements_lst, types_lst, values_lst
    ):
        """
        this method NOT insert data into database and the server just return after accept the request, this method
        should be used to test other time cost in client
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        """
        request = self.gen_insert_records_req(
            device_ids, times, measurements_lst, types_lst, values_lst
        )
        try:
            return Session.verify_success(self.__client.testInsertRecords(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.testInsertRecords(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def gen_insert_record_req(
        self, device_id, timestamp, measurements, data_types, values, is_aligned=False
    ):
        if (len(values) != len(data_types)) or (len(values) != len(measurements)):
            raise RuntimeError(
                "length of data types does not equal to length of values!"
            )
        values_in_bytes = Session.value_to_bytes(data_types, values)
        return TSInsertRecordReq(
            self.__session_id,
            device_id,
            measurements,
            values_in_bytes,
            timestamp,
            is_aligned,
        )

    def gen_insert_str_record_req(
        self, device_id, timestamp, measurements, values, is_aligned=False
    ):
        if len(values) != len(measurements):
            raise RuntimeError(
                "length of measurements does not equal to length of values!"
            )
        return TSInsertStringRecordReq(
            self.__session_id, device_id, measurements, values, timestamp, is_aligned
        )

    def gen_insert_records_req(
        self,
        device_ids,
        times,
        measurements_lst,
        types_lst,
        values_lst,
        is_aligned=False,
    ):
        if (
            (len(device_ids) != len(measurements_lst))
            or (len(times) != len(types_lst))
            or (len(device_ids) != len(times))
            or (len(times) != len(values_lst))
        ):
            raise RuntimeError(
                "deviceIds, times, measurementsList and valuesList's size should be equal"
            )

        value_lst = []
        for values, data_types, measurements in zip(
            values_lst, types_lst, measurements_lst
        ):
            if (len(values) != len(data_types)) or (len(values) != len(measurements)):
                raise RuntimeError(
                    "deviceIds, times, measurementsList and valuesList's size should be equal"
                )
            values_in_bytes = Session.value_to_bytes(data_types, values)
            value_lst.append(values_in_bytes)

        return TSInsertRecordsReq(
            self.__session_id,
            device_ids,
            measurements_lst,
            value_lst,
            times,
            is_aligned,
        )

    def insert_tablet(self, tablet):
        """
        insert one tablet, in a tablet, for each timestamp, the number of measurements is same
            for example three records in the same device can form a tablet:
                timestamps,     m1,    m2,     m3
                         1,  125.3,  True,  text1
                         2,  111.6, False,  text2
                         3,  688.6,  True,  text3
        Notice: From 0.13.0, the tablet can contain empty cell
                The tablet itself is sorted (see docs of Tablet.py)
        :param tablet: a tablet specified above
        """
        request = self.gen_insert_tablet_req(tablet)
        try:
            connection = self.get_connection(tablet.get_insert_target_name())
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertTablet(request)
            )
        except RedirectException as e:
            return self.handle_redirection(
                tablet.get_insert_target_name(), e.redirect_node
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertTablet(request))
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def insert_tablets(self, tablet_lst):
        """
        insert multiple tablets, tablets are independent to each other
        :param tablet_lst: List of tablets
        """
        if self.__enable_redirection:
            request_group = {}
            for i in range(len(tablet_lst)):
                connection = self.get_connection(tablet_lst[i].get_insert_target_name())
                request = request_group.setdefault(
                    connection.client,
                    TSInsertTabletsReq(
                        connection.session_id, [], [], [], [], [], [], False
                    ),
                )
                request.prefixPaths.append(tablet_lst[i].get_insert_target_name())
                request.timestampsList.append(tablet_lst[i].get_binary_timestamps())
                request.measurementsList.append(tablet_lst[i].get_measurements())
                request.valuesList.append(tablet_lst[i].get_binary_values())
                request.sizeList.append(tablet_lst[i].get_row_number())
                request.typesList.append(tablet_lst[i].get_data_types())
            for client, request in request_group.items():
                try:
                    Session.verify_success_with_redirection_for_multi_devices(
                        client.insertTablets(request), request.prefixPaths
                    )
                except RedirectException as e:
                    for device, endpoint in e.device_to_endpoint.items():
                        self.handle_redirection(device, endpoint)
                except TTransport.TException as e:
                    if self.reconnect():
                        try:
                            request.sessionId = self.__session_id
                            Session.verify_success(self.__client.insertTablets(request))
                        except TTransport.TException as e1:
                            raise IoTDBConnectionException(e1) from None
                    else:
                        raise IoTDBConnectionException(
                            self.connection_error_msg()
                        ) from None

            return 0
        else:
            request = self.gen_insert_tablets_req(tablet_lst)
            try:
                return Session.verify_success(self.__client.insertTablets(request))
            except TTransport.TException as e:
                if self.reconnect():
                    try:
                        request.sessionId = self.__session_id
                        return Session.verify_success(
                            self.__client.insertTablets(request)
                        )
                    except TTransport.TException as e1:
                        raise IoTDBConnectionException(e1) from None
                else:
                    raise IoTDBConnectionException(
                        self.connection_error_msg()
                    ) from None

    def insert_aligned_tablet(self, tablet):
        """
        insert one aligned tablet, in a tablet, for each timestamp, the number of measurements is same
            for example three records in the same device can form a tablet:
                timestamps,     m1,    m2,     m3
                         1,  125.3,  True,  text1
                         2,  111.6, False,  text2
                         3,  688.6,  True,  text3
        Notice: From 0.13.0, the tablet can contain empty cell
                The tablet itself is sorted (see docs of Tablet.py)
        :param tablet: a tablet specified above
        """
        request = self.gen_insert_tablet_req(tablet, True)
        try:
            connection = self.get_connection(tablet.get_insert_target_name())
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertTablet(request)
            )
        except RedirectException as e:
            return self.handle_redirection(
                tablet.get_insert_target_name(), e.redirect_node
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertTablet(request))
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def insert_aligned_tablets(self, tablet_lst):
        """
        insert multiple aligned tablets, tablets are independent to each other
        :param tablet_lst: List of tablets
        """
        if self.__enable_redirection:
            request_group = {}
            for i in range(len(tablet_lst)):
                connection = self.get_connection(tablet_lst[i].get_insert_target_name())
                request = request_group.setdefault(
                    connection.client,
                    TSInsertTabletsReq(
                        connection.session_id, [], [], [], [], [], [], True
                    ),
                )
                request.prefixPaths.append(tablet_lst[i].get_insert_target_name())
                request.timestampsList.append(tablet_lst[i].get_binary_timestamps())
                request.measurementsList.append(tablet_lst[i].get_measurements())
                request.valuesList.append(tablet_lst[i].get_binary_values())
                request.sizeList.append(tablet_lst[i].get_row_number())
                request.typesList.append(tablet_lst[i].get_data_types())
            for client, request in request_group.items():
                try:
                    Session.verify_success_with_redirection_for_multi_devices(
                        client.insertTablets(request), request.prefixPaths
                    )
                except RedirectException as e:
                    for device, endpoint in e.device_to_endpoint.items():
                        self.handle_redirection(device, endpoint)
                except TTransport.TException as e:
                    if self.reconnect():
                        try:
                            request.sessionId = self.__session_id
                            Session.verify_success(self.__client.insertTablets(request))
                        except TTransport.TException as e1:
                            raise IoTDBConnectionException(e1) from None
                    else:
                        raise IoTDBConnectionException(
                            self.connection_error_msg()
                        ) from None

            return 0
        else:
            request = self.gen_insert_tablets_req(tablet_lst, True)
            try:
                return Session.verify_success(self.__client.insertTablets(request))
            except TTransport.TException as e:
                if self.reconnect():
                    try:
                        request.sessionId = self.__session_id
                        return Session.verify_success(
                            self.__client.insertTablets(request)
                        )
                    except TTransport.TException as e1:
                        raise IoTDBConnectionException(e1) from None
                else:
                    raise IoTDBConnectionException(
                        self.connection_error_msg()
                    ) from None

    def insert_relational_tablet(self, tablet):
        """
        insert one tablet, for example three column in the table1 can form a tablet:
            timestamps,    id1,  attr1,    m1
                     1,  id:1,  attr:1,   1.0
                     2,  id:1,  attr:1,   2.0
                     3,  id:2,  attr:2,   3.0
        :param tablet: a tablet specified above
        """
        request = self.gen_insert_relational_tablet_req(tablet)
        try:
            connection = self.get_connection(tablet.get_insert_target_name())
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertTablet(request)
            )
        except RedirectException as e:
            return self.handle_redirection(
                tablet.get_insert_target_name(), e.redirect_node
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertTablet(request))
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def insert_records_of_one_device(
        self, device_id, times_list, measurements_list, types_list, values_list
    ):
        # sort by timestamp
        sorted_zipped = sorted(
            zip(times_list, measurements_list, types_list, values_list)
        )
        result = zip(*sorted_zipped)
        times_list, measurements_list, types_list, values_list = [
            list(x) for x in result
        ]

        return self.insert_records_of_one_device_sorted(
            device_id, times_list, measurements_list, types_list, values_list
        )

    def insert_records_of_one_device_sorted(
        self, device_id, times_list, measurements_list, types_list, values_list
    ):
        """
        Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
        executeBatch, we pack some insert request in batch and send them to server. If you want improve
        your performance, please see insertTablet method

        :param device_id: device id
        :param times_list: timestamps list
        :param measurements_list: measurements list
        :param types_list: types list
        :param values_list: values list
        """
        # check parameter
        size = len(times_list)
        if (
            size != len(measurements_list)
            or size != len(types_list)
            or size != len(values_list)
        ):
            raise RuntimeError(
                "insert records of one device error: types, times, measurementsList and valuesList's size should be equal"
            )

        # check sorted
        if not Session.check_sorted(times_list):
            raise RuntimeError(
                "insert records of one device error: timestamp not sorted"
            )

        request = self.gen_insert_records_of_one_device_request(
            device_id, times_list, measurements_list, values_list, types_list
        )
        try:
            connection = self.get_connection(device_id)
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertRecordsOfOneDevice(request)
            )
        except RedirectException as e:
            return self.handle_redirection(device_id, e.redirect_node)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.insertRecordsOfOneDevice(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def insert_aligned_records_of_one_device(
        self, device_id, times_list, measurements_list, types_list, values_list
    ):
        # sort by timestamp
        sorted_zipped = sorted(
            zip(times_list, measurements_list, types_list, values_list)
        )
        result = zip(*sorted_zipped)
        times_list, measurements_list, types_list, values_list = [
            list(x) for x in result
        ]

        return self.insert_aligned_records_of_one_device_sorted(
            device_id, times_list, measurements_list, types_list, values_list
        )

    def insert_aligned_records_of_one_device_sorted(
        self, device_id, times_list, measurements_list, types_list, values_list
    ):
        """
        Insert multiple aligned rows, which can reduce the overhead of network. This method is just like jdbc
        executeBatch, we pack some insert request in batch and send them to server. If you want to improve
        your performance, please see insertTablet method

        :param device_id: device id
        :param times_list: timestamps list
        :param measurements_list: measurements list
        :param types_list: types list
        :param values_list: values list
        """
        # check parameter
        size = len(times_list)
        if (
            size != len(measurements_list)
            or size != len(types_list)
            or size != len(values_list)
        ):
            raise RuntimeError(
                "insert records of one device error: types, times, measurementsList and valuesList's size should be equal"
            )

        # check sorted
        if not Session.check_sorted(times_list):
            raise RuntimeError(
                "insert records of one device error: timestamp not sorted"
            )

        request = self.gen_insert_records_of_one_device_request(
            device_id, times_list, measurements_list, values_list, types_list, True
        )

        # send request
        try:
            connection = self.get_connection(device_id)
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertRecordsOfOneDevice(request)
            )
        except RedirectException as e:
            return self.handle_redirection(device_id, e.redirect_node)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.insertRecordsOfOneDevice(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def gen_insert_records_of_one_device_request(
        self,
        device_id,
        times_list,
        measurements_list,
        values_list,
        types_list,
        is_aligned=False,
    ):
        binary_value_list = []
        for values, data_types, measurements in zip(
            values_list, types_list, measurements_list
        ):
            if (len(values) != len(data_types)) or (len(values) != len(measurements)):
                raise RuntimeError(
                    "insert records of one device error: deviceIds, times, measurementsList and valuesList's size should be equal"
                )
            values_in_bytes = Session.value_to_bytes(data_types, values)
            binary_value_list.append(values_in_bytes)

        return TSInsertRecordsOfOneDeviceReq(
            self.__session_id,
            device_id,
            measurements_list,
            binary_value_list,
            times_list,
            is_aligned,
        )

    def test_insert_tablet(self, tablet):
        """
         this method NOT insert data into database and the server just return after accept the request, this method
         should be used to test other time cost in client
        :param tablet: a tablet of data
        """
        request = self.gen_insert_tablet_req(tablet)
        try:
            return Session.verify_success(self.__client.testInsertTablet(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.testInsertTablet(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def test_insert_tablets(self, tablet_list):
        """
         this method NOT insert data into database and the server just return after accept the request, this method
         should be used to test other time cost in client
        :param tablet_list: List of tablets
        """
        request = self.gen_insert_tablets_req(tablet_list)
        try:
            return Session.verify_success(self.__client.testInsertTablets(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.testInsertTablets(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def gen_insert_tablet_req(self, tablet, is_aligned=False):
        return TSInsertTabletReq(
            self.__session_id,
            tablet.get_insert_target_name(),
            tablet.get_measurements(),
            tablet.get_binary_values(),
            tablet.get_binary_timestamps(),
            tablet.get_data_types(),
            tablet.get_row_number(),
            is_aligned,
        )

    def gen_insert_relational_tablet_req(self, tablet, is_aligned=False):
        return TSInsertTabletReq(
            self.__session_id,
            tablet.get_insert_target_name(),
            tablet.get_measurements(),
            tablet.get_binary_values(),
            tablet.get_binary_timestamps(),
            tablet.get_data_types(),
            tablet.get_row_number(),
            is_aligned,
            True,
            tablet.get_column_categories(),
        )

    def gen_insert_tablets_req(self, tablet_lst, is_aligned=False):
        device_id_lst = []
        measurements_lst = []
        values_lst = []
        timestamps_lst = []
        type_lst = []
        size_lst = []
        for tablet in tablet_lst:
            device_id_lst.append(tablet.get_insert_target_name())
            measurements_lst.append(tablet.get_measurements())
            values_lst.append(tablet.get_binary_values())
            timestamps_lst.append(tablet.get_binary_timestamps())
            type_lst.append(tablet.get_data_types())
            size_lst.append(tablet.get_row_number())
        return TSInsertTabletsReq(
            self.__session_id,
            device_id_lst,
            measurements_lst,
            values_lst,
            timestamps_lst,
            type_lst,
            size_lst,
            is_aligned,
        )

    def execute_query_statement(self, sql, timeout=0):
        """
        execute query sql statement and returns SessionDataSet
        :param sql: String, query sql statement
        :param timeout:
        :return: SessionDataSet, contains query results and relevant info (see SessionDataSet.py):
        """
        request = TSExecuteStatementReq(
            self.__session_id, sql, self.__statement_id, self.__fetch_size, timeout
        )
        try:
            resp = self.__client.executeQueryStatement(request)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    request.statementId = self.__statement_id
                    resp = self.__client.executeQueryStatement(request)
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

        Session.verify_success(resp.status)
        return SessionDataSet(
            sql,
            resp.columns,
            resp.dataTypeList,
            resp.columnNameIndexMap,
            resp.queryId,
            self.__client,
            self.__statement_id,
            self.__session_id,
            resp.queryDataSet,
            resp.ignoreTimeStamp,
        )

    def execute_non_query_statement(self, sql):
        """
        execute non-query sql statement
        :param sql: String, non-query sql statement
        """
        request = TSExecuteStatementReq(self.__session_id, sql, self.__statement_id)
        try:
            resp = self.__client.executeUpdateStatement(request)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    request.statementId = self.__statement_id
                    resp = self.__client.executeUpdateStatement(request)
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

        previous_db = self.database
        if resp.database is not None:
            self.database = resp.database
        if previous_db != self.database and self.__endpoint_to_connection is not None:
            iterator = iter(self.__endpoint_to_connection.items())
            for entry in list(iterator):
                endpoint, connection = entry
                if connection != self.__default_connection:
                    try:
                        connection.change_database(sql)
                    except Exception as e:
                        self.__endpoint_to_connection.pop(endpoint)
        return Session.verify_success(resp.status)

    def execute_statement(self, sql: str, timeout=0):
        request = TSExecuteStatementReq(
            self.__session_id, sql, self.__statement_id, timeout
        )
        try:
            resp = self.__client.executeStatement(request)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    request.statementId = self.__statement_id
                    resp = self.__client.executeStatement(request)
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

        Session.verify_success(resp.status)
        if resp.columns:
            return SessionDataSet(
                sql,
                resp.columns,
                resp.dataTypeList,
                resp.columnNameIndexMap,
                resp.queryId,
                self.__client,
                self.__statement_id,
                self.__session_id,
                resp.queryDataSet,
                resp.ignoreTimeStamp,
            )
        else:
            return None

    @staticmethod
    def value_to_bytes(data_types, values):
        format_str_list = [">"]
        values_tobe_packed = []
        for data_type, value in zip(data_types, values):
            # BOOLEAN
            if data_type == 0:
                format_str_list.append("c?")
                values_tobe_packed.append(b"\x00")
                values_tobe_packed.append(value)
            # INT32
            elif data_type == 1:
                format_str_list.append("ci")
                values_tobe_packed.append(b"\x01")
                values_tobe_packed.append(value)
            # INT64
            elif data_type == 2:
                format_str_list.append("cq")
                values_tobe_packed.append(b"\x02")
                values_tobe_packed.append(value)
            # FLOAT
            elif data_type == 3:
                format_str_list.append("cf")
                values_tobe_packed.append(b"\x03")
                values_tobe_packed.append(value)
            # DOUBLE
            elif data_type == 4:
                format_str_list.append("cd")
                values_tobe_packed.append(b"\x04")
                values_tobe_packed.append(value)
            # TEXT
            elif data_type == 5:
                if isinstance(value, str):
                    value_bytes = bytes(value, "utf-8")
                else:
                    value_bytes = value
                format_str_list.append("ci")
                format_str_list.append(str(len(value_bytes)))
                format_str_list.append("s")
                values_tobe_packed.append(b"\x05")
                values_tobe_packed.append(len(value_bytes))
                values_tobe_packed.append(value_bytes)
            # TIMESTAMP
            elif data_type == 8:
                format_str_list.append("cq")
                values_tobe_packed.append(b"\x08")
                values_tobe_packed.append(value)
            # DATE
            elif data_type == 9:
                format_str_list.append("ci")
                values_tobe_packed.append(b"\x09")
                values_tobe_packed.append(parse_date_to_int(value))
            # BLOB
            elif data_type == 10:
                format_str_list.append("ci")
                format_str_list.append(str(len(value)))
                format_str_list.append("s")
                values_tobe_packed.append(b"\x0a")
                values_tobe_packed.append(len(value))
                values_tobe_packed.append(value)
            # STRING
            elif data_type == 11:
                if isinstance(value, str):
                    value_bytes = bytes(value, "utf-8")
                else:
                    value_bytes = value
                format_str_list.append("ci")
                format_str_list.append(str(len(value_bytes)))
                format_str_list.append("s")
                values_tobe_packed.append(b"\x0b")
                values_tobe_packed.append(len(value_bytes))
                values_tobe_packed.append(value_bytes)
            else:
                raise RuntimeError("Unsupported data type:" + str(data_type))
        format_str = "".join(format_str_list)
        return struct.pack(format_str, *values_tobe_packed)

    def get_time_zone(self):
        if self.__zone_id is not None:
            return self.__zone_id
        try:
            resp = self.__client.getTimeZone(self.__session_id)
        except TTransport.TException as e:
            raise IoTDBConnectionException(
                "Could not get time zone because: ", e
            ) from None
        return resp.timeZone

    def set_time_zone(self, zone_id):
        request = TSSetTimeZoneReq(self.__session_id, zone_id)
        try:
            status = self.__client.setTimeZone(request)
            logger.debug(
                "setting time zone_id as {}, message: {}".format(
                    zone_id, status.message
                )
            )
        except TTransport.TException as e:
            raise IoTDBConnectionException(
                "Could not set time zone because: ", e
            ) from None
        self.__zone_id = zone_id

    @staticmethod
    def check_sorted(timestamps):
        for i in range(1, len(timestamps)):
            if timestamps[i] < timestamps[i - 1]:
                return False
        return True

    @staticmethod
    def verify_success(status: TSStatus):
        """
        verify success of operation
        :param status: execution result status
        """
        if status.code == Session.MULTIPLE_ERROR:
            Session.verify_success_by_list(status.subStatus)
            return 0
        if (
            status.code == Session.SUCCESS_STATUS
            or status.code == Session.REDIRECTION_RECOMMEND
        ):
            return 0

        raise RuntimeError(f"{status.code}: {status.message}")

    @staticmethod
    def verify_success_by_list(status_list: list):
        """
        verify success of operation
        :param status_list: execution result status
        """
        error_messages = [
            status.message
            for status in status_list
            if status.code
            not in {Session.SUCCESS_STATUS, Session.REDIRECTION_RECOMMEND}
        ]
        if error_messages:
            message = f"{Session.MULTIPLE_ERROR}: {'; '.join(error_messages)}"
            raise RuntimeError(message)

    @staticmethod
    def verify_success_with_redirection(status: TSStatus):
        Session.verify_success(status)
        if status.redirectNode is not None:
            raise RedirectException(status.redirectNode)
        return 0

    @staticmethod
    def verify_success_with_redirection_for_multi_devices(
        status: TSStatus, devices: list
    ):
        Session.verify_success(status)
        if (
            status.code == Session.MULTIPLE_ERROR
            or status.code == Session.REDIRECTION_RECOMMEND
        ):
            device_to_endpoint = {}
            for i in range(len(status.subStatus)):
                if status.subStatus[i].redirectNode is not None:
                    device_to_endpoint[devices[i]] = status.subStatus[i].redirectNode
            raise RedirectException(device_to_endpoint)

    def execute_raw_data_query(
        self, paths: list, start_time: int, end_time: int
    ) -> SessionDataSet:
        """
        execute query statement and returns SessionDataSet
        :param paths: String path list
        :param start_time: Query start time
        :param end_time: Query end time
        :return: SessionDataSet, contains query results and relevant info (see SessionDataSet.py)
        """
        request = TSRawDataQueryReq(
            self.__session_id,
            paths,
            self.__fetch_size,
            startTime=start_time,
            endTime=end_time,
            statementId=self.__statement_id,
            enableRedirectQuery=False,
        )
        try:
            resp = self.__client.executeRawDataQuery(request)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    request.statementId = self.__statement_id
                    resp = self.__client.executeRawDataQuery(request)
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None
        Session.verify_success(resp.status)
        return SessionDataSet(
            "",
            resp.columns,
            resp.dataTypeList,
            resp.columnNameIndexMap,
            resp.queryId,
            self.__client,
            self.__statement_id,
            self.__session_id,
            resp.queryDataSet,
            resp.ignoreTimeStamp,
        )

    def execute_last_data_query(self, paths: list, last_time: int) -> SessionDataSet:
        """
        execute query statement and returns SessionDataSet
        :param paths: String path list
        :param last_time: Query last time
        :return: SessionDataSet, contains query results and relevant info (see SessionDataSet.py)
        """
        request = TSLastDataQueryReq(
            self.__session_id,
            paths,
            self.__fetch_size,
            last_time,
            self.__statement_id,
            enableRedirectQuery=False,
        )
        try:
            resp = self.__client.executeLastDataQuery(request)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    request.statementId = self.__statement_id
                    resp = self.__client.executeLastDataQuery(request)
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None
        Session.verify_success(resp.status)
        return SessionDataSet(
            "",
            resp.columns,
            resp.dataTypeList,
            resp.columnNameIndexMap,
            resp.queryId,
            self.__client,
            self.__statement_id,
            self.__session_id,
            resp.queryDataSet,
            resp.ignoreTimeStamp,
        )

    def insert_string_records_of_one_device(
        self,
        device_id: str,
        times: list,
        measurements_list: list,
        values_list: list,
        have_sorted: bool = False,
    ):
        """
        insert multiple row of string record into database:
                 timestamp,     m1,    m2,     m3
                         0,  text1,  text2, text3
        :param device_id: String, device id
        :param times: Timestamp list
        :param measurements_list: Measurements list
        :param values_list: Value list
        :param have_sorted: have these list been sorted by timestamp
        """
        if (len(times) != len(measurements_list)) or (len(times) != len(values_list)):
            raise RuntimeError(
                "insert records of one device error: times, measurementsList and valuesList's size should be equal!"
            )
        request = self.gen_insert_string_records_of_one_device_request(
            device_id, times, measurements_list, values_list, have_sorted, False
        )
        try:
            connection = self.get_connection(device_id)
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertStringRecordsOfOneDevice(request)
            )
        except RedirectException as e:
            return self.handle_redirection(device_id, e.redirect_node)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.insertStringRecordsOfOneDevice(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def insert_aligned_string_records_of_one_device(
        self,
        device_id: str,
        times: list,
        measurements_list: list,
        values: list,
        have_sorted: bool = False,
    ):
        if (len(times) != len(measurements_list)) or (len(times) != len(values)):
            raise RuntimeError(
                "insert records of one device error: times, measurementsList and valuesList's size should be equal!"
            )
        request = self.gen_insert_string_records_of_one_device_request(
            device_id, times, measurements_list, values, have_sorted, True
        )
        try:
            connection = self.get_connection(device_id)
            request.sessionId = connection.session_id
            return Session.verify_success_with_redirection(
                connection.client.insertStringRecordsOfOneDevice(request)
            )
        except RedirectException as e:
            return self.handle_redirection(device_id, e.redirect_node)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.insertStringRecordsOfOneDevice(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def reconnect(self):
        if self.__hosts is None:
            return False
        connected = False
        for i in range(1, self.RETRY_NUM + 1):
            if (
                self.__default_connection is not None
                and self.__default_connection.transport is not None
            ):
                self.__default_connection.transport.close()
            curr_host_index = random.randint(0, len(self.__hosts) - 1)
            try_host_num = 0
            j = curr_host_index
            while j < len(self.__hosts):
                if try_host_num == len(self.__hosts):
                    break
                self.__default_endpoint = TEndPoint(self.__hosts[j], self.__ports[j])
                if j == len(self.__hosts) - 1:
                    j = -1
                try_host_num += 1
                try:
                    self.__default_connection = self.init_connection(
                        self.__default_endpoint
                    )
                    self.__client = self.__default_connection.client
                    self.__session_id = self.__default_connection.session_id
                    self.__statement_id = self.__default_connection.statement_id
                    connected = True
                    if self.__enable_redirection:
                        self.__endpoint_to_connection = {
                            str(self.__default_endpoint): self.__default_connection
                        }
                except IoTDBConnectionException:
                    pass
                    j += 1
                    continue
                break
            if connected:
                break
        return connected

    def connection_error_msg(self):
        if self.__hosts is None:
            msg = "Could not connect to [('%s', %s)]" % (self.__host, self.__port)
        else:
            node_list = []
            for i in range(len(self.__hosts)):
                node_list.append("('%s', %s)" % (self.__hosts[i], self.__ports[i]))
            msg = "Could not connect to any of [%s]" % ", ".join(node_list)
        return msg

    def get_connection(self, device_id):
        if (
            self.__enable_redirection
            and len(self.__device_id_to_endpoint) != 0
            and device_id in self.__device_id_to_endpoint
        ):
            endpoint = self.__device_id_to_endpoint[device_id]
            if str(endpoint) in self.__endpoint_to_connection:
                return self.__endpoint_to_connection[str(endpoint)]
        return self.__default_connection

    def handle_redirection(self, device_id, endpoint: TEndPoint):
        if self.__enable_redirection:
            if endpoint.ip == "0.0.0.0":
                return 0
            if (
                device_id not in self.__device_id_to_endpoint
                or self.__device_id_to_endpoint[device_id] != endpoint
            ):
                self.__device_id_to_endpoint[device_id] = endpoint
            if str(endpoint) in self.__endpoint_to_connection:
                connection = self.__endpoint_to_connection[str(endpoint)]
            else:
                try:
                    connection = self.init_connection(endpoint)
                except Exception:
                    connection = None
                self.__endpoint_to_connection[str(endpoint)] = connection
            if connection is None:
                self.__device_id_to_endpoint.pop(device_id)
        return 0

    def gen_insert_string_records_of_one_device_request(
        self,
        device_id,
        times,
        measurements_list,
        values_list,
        have_sorted,
        is_aligned=False,
    ):
        if (len(times) != len(measurements_list)) or (len(times) != len(values_list)):
            raise RuntimeError(
                "insert records of one device error: times, measurementsList and valuesList's size should be equal!"
            )
        if not Session.check_sorted(times):
            # sort by timestamp
            sorted_zipped = sorted(zip(times, measurements_list, values_list))
            result = zip(*sorted_zipped)
            times_list, measurements_list, values_list = [list(x) for x in result]
        request = TSInsertStringRecordsOfOneDeviceReq(
            self.__session_id,
            device_id,
            measurements_list,
            values_list,
            times,
            is_aligned,
        )
        return request

    def __has_none_value(self, values_list) -> bool:
        for item in values_list:
            if isinstance(item, list):
                if self.__has_none_value(item):
                    return True
            elif item is None:
                return True
        return False

    @staticmethod
    def __filter_lists_by_values(
        device_lst, time_lst, measurements_lst, types_lst, values_lst
    ):
        filtered_devices = []
        filtered_times = []
        filtered_measurements = []
        filtered_types = []
        filtered_values = []

        for device, time_, measurements, types, values in zip(
            device_lst, time_lst, measurements_lst, types_lst, values_lst
        ):
            filtered_row = [
                (m, t, v)
                for m, t, v in zip(measurements, types, values)
                if v is not None
            ]
            if filtered_row:
                f_measurements, f_types, f_values = zip(*filtered_row)
                filtered_measurements.append(list(f_measurements))
                filtered_types.append(list(f_types))
                filtered_values.append(list(f_values))
                filtered_devices.append(device)
                filtered_times.append(time_)

        return (
            filtered_devices,
            filtered_times,
            filtered_measurements,
            filtered_types,
            filtered_values,
        )

    def create_schema_template(self, template: Template):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        create device template, users using this method should use the template class as an argument
        :param template: The template contains multiple child node(see Template.py)
        """
        bytes_array = template.serialize
        request = TSCreateSchemaTemplateReq(
            self.__session_id, template.get_name(), bytes_array
        )
        try:
            return Session.verify_success(self.__client.createSchemaTemplate(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.createSchemaTemplate(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def drop_schema_template(self, template_name: str):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        drop device template, this method should be used to the template unset anything
        :param template_name: template name
        """
        request = TSDropSchemaTemplateReq(self.__session_id, template_name)
        try:
            return Session.verify_success(self.__client.dropSchemaTemplate(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.dropSchemaTemplate(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def add_measurements_in_template(
        self,
        template_name: str,
        measurements_path: list,
        data_types: list,
        encodings: list,
        compressors: list,
        is_aligned: bool = False,
    ):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        add measurements in the template, the template must already create. This function adds some measurements' node.
        :param template_name: template name, string list, like ["name_x", "name_y", "name_z"]
        :param measurements_path: when ths is_aligned is True, recommend the name like a.b,
        like [python.x, python.y, iotdb.z]
        :param data_types: using TSDataType(see IoTDBConstants.py)
        :param encodings: using TSEncoding(see IoTDBConstants.py)
        :param compressors: using Compressor(see IoTDBConstants.py)
        :param is_aligned: True is aligned, False is unaligned
        """
        request = TSAppendSchemaTemplateReq(
            self.__session_id,
            template_name,
            is_aligned,
            measurements_path,
            data_types,
            encodings,
            compressors,
        )
        try:
            return Session.verify_success(self.__client.appendSchemaTemplate(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.appendSchemaTemplate(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def delete_node_in_template(self, template_name: str, path: str):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        delete a node in the template, this node must be already in the template
        :param template_name: template name
        :param path: measurements path
        """
        request = TSPruneSchemaTemplateReq(self.__session_id, template_name, path)
        try:
            return Session.verify_success(self.__client.pruneSchemaTemplate(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.pruneSchemaTemplate(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def set_schema_template(self, template_name, prefix_path):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        set template in prefix path, template already exit, prefix path is not measurements
        :param template_name: template name
        :param prefix_path: prefix path
        """
        request = TSSetSchemaTemplateReq(self.__session_id, template_name, prefix_path)
        try:
            return Session.verify_success(self.__client.setSchemaTemplate(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.setSchemaTemplate(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def unset_schema_template(self, template_name, prefix_path):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        unset device template from prefix path, this method unsetting the template from entities,
        which have already inserted records using the template, is not supported.
        :param template_name: template name
        :param prefix_path:
        """
        request = TSUnsetSchemaTemplateReq(
            self.__session_id, prefix_path, template_name
        )
        try:
            return Session.verify_success(self.__client.unsetSchemaTemplate(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.unsetSchemaTemplate(request)
                    )
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def count_measurements_in_template(self, template_name: str):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        drop device template, this method should be used to the template unset anything
        :param template_name: template name
        """
        request = TSQueryTemplateReq(
            self.__session_id,
            template_name,
            TemplateQueryType.COUNT_MEASUREMENTS.value,
        )
        try:
            response = self.__client.querySchemaTemplate(request)
            return response.count
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    response = self.__client.querySchemaTemplate(request)
                    Session.verify_success(response.status)
                    return response.count
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def is_measurement_in_template(self, template_name: str, path: str):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        judge the node in the template is measurement or not, this node must in the template
        :param template_name: template name
        :param path:
        """
        request = TSQueryTemplateReq(
            self.__session_id,
            template_name,
            TemplateQueryType.IS_MEASUREMENT.value,
            path,
        )
        try:
            response = self.__client.querySchemaTemplate(request)
            Session.verify_success(response.status)
            return response.result
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    response = self.__client.querySchemaTemplate(request)
                    Session.verify_success(response.status)
                    return response.result
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def is_path_exist_in_template(self, template_name: str, path: str):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        judge whether the node is a measurement or not in the template, this node must be in the template
        :param template_name: template name
        :param path:
        """
        request = TSQueryTemplateReq(
            self.__session_id, template_name, TemplateQueryType.PATH_EXIST.value, path
        )
        try:
            response = self.__client.querySchemaTemplate(request)
            Session.verify_success(response.status)
            return response.result
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    response = self.__client.querySchemaTemplate(request)
                    Session.verify_success(response.status)
                    return response.result
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def show_measurements_in_template(self, template_name: str, pattern: str = ""):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        show all measurements under the pattern in template
        :param template_name: template name
        :param pattern: parent path, if defaulted, show all measurements
        """
        request = TSQueryTemplateReq(
            self.__session_id,
            template_name,
            TemplateQueryType.SHOW_MEASUREMENTS.value,
            pattern,
        )
        try:
            response = self.__client.querySchemaTemplate(request)
            Session.verify_success(response.status)
            return response.measurements
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    response = self.__client.querySchemaTemplate(request)
                    Session.verify_success(response.status)
                    return response.measurements
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def show_all_templates(self):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        show all device templates
        """
        request = TSQueryTemplateReq(
            self.__session_id,
            "",
            TemplateQueryType.SHOW_TEMPLATES.value,
        )
        try:
            response = self.__client.querySchemaTemplate(request)
            Session.verify_success(response.status)
            return response.measurements
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    response = self.__client.querySchemaTemplate(request)
                    Session.verify_success(response.status)
                    return response.measurements
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def show_paths_template_set_on(self, template_name):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        show the path prefix where a device template is set
        :param template_name:
        """
        request = TSQueryTemplateReq(
            self.__session_id, template_name, TemplateQueryType.SHOW_SET_TEMPLATES.value
        )
        try:
            response = self.__client.querySchemaTemplate(request)
            Session.verify_success(response.status)
            return response.measurements
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    response = self.__client.querySchemaTemplate(request)
                    Session.verify_success(response.status)
                    return response.measurements
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None

    def show_paths_template_using_on(self, template_name):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        """
        show the path prefix where a device template is used
        :param template_name:
        """
        request = TSQueryTemplateReq(
            self.__session_id,
            template_name,
            TemplateQueryType.SHOW_USING_TEMPLATES.value,
        )
        try:
            response = self.__client.querySchemaTemplate(request)
            Session.verify_success(response.status)
            return response.measurements
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    response = self.__client.querySchemaTemplate(request)
                    Session.verify_success(response.status)
                    return response.measurements
                except TTransport.TException as e1:
                    raise IoTDBConnectionException(e1) from None
            else:
                raise IoTDBConnectionException(self.connection_error_msg()) from None


class SessionConnection(object):
    def __init__(
        self,
        client,
        transport,
        session_id,
        statement_id,
    ):
        self.client = client
        self.transport = transport
        self.session_id = session_id
        self.statement_id = statement_id

    def change_database(self, sql):
        try:
            self.client.executeUpdateStatement(
                TSExecuteStatementReq(self.session_id, sql, self.statement_id)
            )
        except TTransport.TException as e:
            raise IoTDBConnectionException(
                "failed to change database",
                e,
            ) from None

    def close_connection(self, req):
        try:
            self.client.closeSession(req)
        except TTransport.TException as e:
            raise IoTDBConnectionException(
                "Error occurs when closing session at server. Maybe server is down. Error message: ",
                e,
            ) from None
        finally:
            if self.transport is not None:
                self.transport.close()


class RedirectException(Exception):
    def __init__(self, redirect_info):
        Exception.__init__(self)
        if isinstance(redirect_info, TEndPoint):
            self.redirect_node = redirect_info
        else:
            self.device_to_endpoint = redirect_info
