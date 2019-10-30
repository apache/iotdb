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

from rpc.TSIService import Client, TSCreateTimeseriesReq, TSInsertionReq, TSBatchInsertionReq, TSExecuteStatementReq,\
    TS_SessionHandle, TSHandleIdentifier, TSOpenSessionReq, TSQueryDataSet, TSFetchResultsReq, TSCloseOperationReq,\
    TSCloseSessionReq

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
    client.openSession(TSOpenSessionReq(username=username, password=password))

    # This is necessary for resource control
    stmtId = client.requestStatementId()

    # These two fields do not matter
    handle = TS_SessionHandle(TSHandleIdentifier(b'uuid', b'secret'))

    # create a storage group
    status = client.setStorageGroup("root.group1")
    print(status.statusType)

    # create timeseries
    status = client.createTimeseries(TSCreateTimeseriesReq("root.group1.s1", TSDataType['INT64'], TSEncoding['PLAIN'],
                                                           Compressor['UNCOMPRESSED']))
    print(status.statusType)
    status = client.createTimeseries(TSCreateTimeseriesReq("root.group1.s2", TSDataType['INT64'], TSEncoding['PLAIN'],
                                                           Compressor['UNCOMPRESSED']))
    print(status.statusType)
    status = client.createTimeseries(TSCreateTimeseriesReq("root.group1.s3", TSDataType['INT64'], TSEncoding['PLAIN'],
                                                           Compressor['UNCOMPRESSED']))
    print(status.statusType)

    # insert a single row
    status = client.insertRow(TSInsertionReq("root.group1", ["s1", "s2", "s3"], ["1", "11", "111"], 1, 1))
    print(status.statusType)

    # insert multiple rows, this interface is more efficient
    stringValues = bytearray()
    times = bytearray()
    deviceId = "root.group1"
    measurements = ["s1", "s2", "s3"]
    dataSize = 3
    dataTypes = [TSDataType['INT64'], TSDataType['INT64'], TSDataType['INT64']]
    # the first 3 belong to 's1', the mid 3 belong to 's2', the last 3 belong to 's3'
    stringValues.extend(struct.pack('>qqqqqqqqq', 2, 3, 4, 22, 33, 44, 222, 333, 444))
    times.extend(struct.pack('>qqq', 2, 3, 4))
    resp = client.insertBatch(TSBatchInsertionReq(deviceId, measurements, stringValues, times,
                                                  dataTypes, dataSize))
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
    stmtHandle = resp.operationHandle
    status = resp.status
    print(status.statusType)
    while True:
        rst = client.fetchResults(TSFetchResultsReq(stmt, fetchSize, queryId)).queryDataSet
        records = rst.records
        if len(records) == 0:
            break
        for record in records:
            print(record)

    # do not forget to close it when a query is over
    client.closeOperation(TSCloseOperationReq(stmtHandle, queryId, stmtId))

    # and do not forget to close the session before exiting
    client.closeSession(TSCloseSessionReq(handle))


