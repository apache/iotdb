#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#

import sys, struct
sys.path.append("../target")

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from rpc.TSIService import Client, TSCreateTimeseriesReq, TSInsertionReq, \
    TSBatchInsertionReq, TSExecuteStatementReq, \
    TS_SessionHandle, TSHandleIdentifier, TSOpenSessionReq, TSQueryDataSet, \
    TSFetchResultsReq, TSCloseOperationReq, \
    TSCloseSessionReq, TSProtocolVersion

TSDataType = {
    'BOOLEAN' : 0,
    'INT32' : 1,
    'INT64' : 2,
    'FLOAT' : 3,
    'DOUBLE' : 4,
    'TEXT' : 5
}

TSEncoding = {
    'PLAIN' : 0,
    'PLAIN_DICTIONARY' : 1,
    'RLE' : 2,
    'DIFF' : 3,
    'TS_2DIFF' : 4,
    'BITMAP' : 5,
    'GORILLA' : 6,
    'REGULAR' : 7
}

Compressor = {
    'UNCOMPRESSED' : 0,
    'SNAPPY' : 1,
    'GZIP' : 2,
    'LZO' : 3,
    'SDT' : 4,
    'PAA' : 5,
    'PLA' : 6
}


def convertQueryDataSet(queryDataSet, dataTypeList):
    bytes = queryDataSet.values
    row_count = queryDataSet.rowCount
    time_bytes = bytes[:8*row_count]
    value_bytes = bytes[8*row_count:]
    time_unpack_str = '>' + str(row_count) + 'q'
    records = []
    times = struct.unpack(time_unpack_str, time_bytes)
    for i in range(row_count):
        records.append([times[i]])

    for i in range(len(dataTypeList)):
        type = dataTypeList[i]
        for j in range(row_count):
            is_null = value_bytes[0]
            value_bytes = value_bytes[1:]
            if is_null == 1:
                records[j].append('null')
            else:
                if type == 'BOOLEAN':
                    value = value_bytes[:1]
                    value_bytes = value_bytes[1:]
                    records[j].append(struct.unpack('>?', value)[0])
                elif type == 'INT32':
                    value = value_bytes[:4]
                    value_bytes = value_bytes[4:]
                    records[j].append(struct.unpack('>i', value)[0])
                elif type == 'INT64':
                    value = value_bytes[:8]
                    value_bytes = value_bytes[8:]
                    records[j].append(struct.unpack('>q', value)[0])
                elif type == 'FLOAT':
                    value = value_bytes[:4]
                    value_bytes = value_bytes[4:]
                    records[j].append(struct.unpack('>f', value)[0])
                elif type == 'DOUBLE':
                    value = value_bytes[:8]
                    value_bytes = value_bytes[8:]
                    records[j].append(struct.unpack('>d', value)[0])
                elif type == 'TEXT':
                    size = value_bytes[:4]
                    value_bytes = value_bytes[4:]
                    size = struct.unpack('>i', size)[0]
                    records[j].append(value_bytes[:size])
                    value_bytes = value_bytes[size:]
    return records


if __name__ == '__main__':
    ip = "localhost"
    port = "6667"
    username = 'root'
    password = 'root'
    # Make socket
    transport = TSocket.TSocket(ip, port)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = Client(protocol)

    # Connect!
    transport.open()

    # Authentication
    clientProtocol = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1
    resp = client.openSession(TSOpenSessionReq(client_protocol=clientProtocol,
                                               username=username,
                                               password=password))
    if resp.serverProtocolVersion != clientProtocol:
        print('Inconsistent protocol, server version: %d, client version: %d'
              % (resp.serverProtocolVersion, clientProtocol))
        exit()
    handle = resp.sessionHandle

    # This is necessary for resource control
    stmtId = client.requestStatementId()

    # create a storage group
    status = client.setStorageGroup("root.group1")
    print(status.statusType)

    # create timeseries
    status = client.createTimeseries(TSCreateTimeseriesReq("root.group1.s1",
                                                           TSDataType['INT64'],
                                                           TSEncoding['PLAIN'],
                                                           Compressor['UNCOMPRESSED']))
    print(status.statusType)
    status = client.createTimeseries(TSCreateTimeseriesReq("root.group1.s2",
                                                           TSDataType['INT32'],
                                                           TSEncoding['PLAIN'],
                                                           Compressor['UNCOMPRESSED']))
    print(status.statusType)
    status = client.createTimeseries(TSCreateTimeseriesReq("root.group1.s3",
                                                           TSDataType['DOUBLE'],
                                                           TSEncoding['PLAIN'],
                                                           Compressor['UNCOMPRESSED']))
    print(status.statusType)
    status = client.createTimeseries(TSCreateTimeseriesReq("root.group1.s4",
                                                           TSDataType['FLOAT'],
                                                           TSEncoding['PLAIN'],
                                                           Compressor['UNCOMPRESSED']))
    print(status.statusType)
    status = client.createTimeseries(TSCreateTimeseriesReq("root.group1.s5",
                                                           TSDataType['BOOLEAN'],
                                                           TSEncoding['PLAIN'],
                                                           Compressor['UNCOMPRESSED']))
    print(status.statusType)
    status = client.createTimeseries(TSCreateTimeseriesReq("root.group1.s6",
                                                           TSDataType['TEXT'],
                                                           TSEncoding['PLAIN'],
                                                           Compressor['UNCOMPRESSED']))
    print(status.statusType)

    deviceId = "root.group1"
    measurements = ["s1", "s2", "s3", "s4", "s5", "s6"]

    # insert a single row
    values = ["1", "11", "1.1", "11.1", "TRUE", "\'text0\'"]
    timestamp = 1
    status = client.insertRow(TSInsertionReq(deviceId, measurements,
                                             values, timestamp, stmtId))
    print(status.statusType)

    # insert multiple rows, this interface is more efficient
    values = bytearray()
    times = bytearray()

    rowCnt = 3
    dataTypes = [TSDataType['INT64'], TSDataType['INT32'], TSDataType['DOUBLE'],
                 TSDataType['FLOAT'], TSDataType['BOOLEAN'], TSDataType['TEXT']]
    # the first 3 belong to 's1', the second 3 belong to 's2'... the last 3
    # belong to 's6'
    # to transfer a string, you must first send its length and then its bytes
    # (like the last 3 'i7s'). Text values should start and end with ' or ".
    # IoTDB use big endian in rpc
    value_pack_str = '>3q3i3d3f3bi7si7si7s'
    time_pack_str = '>3q'
    encoding = 'utf-8'
    values.extend(struct.pack(value_pack_str, 2, 3, 4, 22, 33, 44, 2.2, 3.3,
                              4.4, 22.2, 33.3, 44.4, True, True, False,
                              len(bytes('\'text1\'', encoding)),
                              bytes('\'text1\'', encoding),
                              len(bytes('\'text2\'', encoding)),
                              bytes('\'text2\'', encoding),
                              len(bytes('\'text3\'', encoding)),
                              bytes('\'text3\'', encoding)))
    times.extend(struct.pack(time_pack_str, 2, 3, 4))
    resp = client.insertBatch(TSBatchInsertionReq(deviceId, measurements, values,
                                                  times, dataTypes, rowCnt))
    status = resp.status
    print(status.statusType)

    # execute deletion (or other statements)
    resp = client.executeStatement(TSExecuteStatementReq(handle, "DELETE FROM root.group1 where time < 2"))
    status = resp.status
    print(status.statusType)

    # query the data
    stmt = "SELECT * FROM root.group1"
    fetchSize = 2
    # this is also for resource control, make sure different queries will not use the same id at the same time
    queryId = 1
    resp = client.executeQueryStatement(TSExecuteStatementReq(handle, stmt))
    # headers
    dataTypeList = resp.dataTypeList
    print(resp.columns)
    print(dataTypeList)

    stmtHandle = resp.operationHandle
    status = resp.status
    print(status.statusType)
    while True:
        rst = client.fetchResults(TSFetchResultsReq(stmt, fetchSize, queryId)).queryDataSet
        records = convertQueryDataSet(rst, dataTypeList)
        if len(records) == 0:
            break
        for record in records:
            print(record)

    # do not forget to close it when a query is over
    client.closeOperation(TSCloseOperationReq(stmtHandle, queryId, stmtId))

    # and do not forget to close the session before exiting
    client.closeSession(TSCloseSessionReq(handle))


