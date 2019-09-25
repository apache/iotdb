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
    TS_SessionHandle, TSHandleIdentifier, TSOpenSessionReq, TSQueryDataSet
if __name__ == '__main__':
    # Make socket
    transport = TSocket.TSocket("localhost", "6667")

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = Client(protocol)

    # Connect!
    transport.open()

    client.openSession(TSOpenSessionReq(username='root', password='root'))

    handle = TS_SessionHandle(TSHandleIdentifier(b'1', b'1'))

    client.setStorageGroup("root.group1")
    client.createTimeseries(TSCreateTimeseriesReq("root.group1.s1", 2, 0, 0))
    client.createTimeseries(TSCreateTimeseriesReq("root.group1.s2", 2, 0, 0))
    client.createTimeseries(TSCreateTimeseriesReq("root.group1.s3", 2, 0, 0))

    client.insertRow(TSInsertionReq("root.group1", ["s1", "s2", "s3"], ["1", "11", "111"], 1, 1))

    values = bytearray()
    times = bytearray()
    values.extend(struct.pack('<qqqqqqqqq', 2, 3, 4, 22, 33, 44, 222, 333, 444))
    times.extend(struct.pack('<qqq', 2, 3, 4))
    client.insertBatch(TSBatchInsertionReq("root.group1", ["s1", "s2", "s3"], values, times, [2, 2, 2], 3))

    rst = client.executeQueryStatement(TSExecuteStatementReq(handle, "SELECT * FROM root.group1"))
    print rst


