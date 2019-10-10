<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# IoTDB Examples

These examples give a quick overview of the IoTDB JDBC. IoTDB offers standard JDBC for users to interact with IoTDB, other language versions are comming soon.

To use IoTDB, you need to set storage group (detail concept about storage group, please view our documentations) for your timeseries. Then you need to create specific timeseries((detail concept about storage group, please view our documentations)) according to its data type, name, etc. After that, inserting and query data is allowed. In this page, we will show an basic example using IoTDB JDBC.

## IoTDB Hello World

``` JAVA
/**
 * The class is to show how to write and read date from IoTDB through JDBC
 */
package com.tsinghua.iotdb.demo;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class IotdbHelloWorld {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Connection connection = null;
		Statement statement = null;
		try {

            // 1. load JDBC driver of IoTDB
            Class.forName("org.apache.iotdb.iotdb.jdbc.IoTDBDriver");
            // 2. DriverManager connect to IoTDB
            connection = DriverManager.getConnection("jdbc:iotdb://localhost:6667/", "root", "root");
            // 3. Create statement
            statement = connection.createStatement();
            // 4. Set storage group
            statement.execute("set storage group to root.vehicle.sensor");
            // 5. Create timeseries
            statement.execute("CREATE TIMESERIES root.vehicle.sensor.sensor0 WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
            // 6. Insert data to IoTDB
            statement.execute("INSERT INTO root.vehicle.sensor(timestamp, sensor0) VALUES (2018/10/24 19:33:00, 142)");
            // 7. Query data
            String sql = "select * from root.vehicle.sensor";
            String path = "root.vehicle.sensor.sensor0";
            boolean hasResultSet = statement.execute(sql);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            if (hasResultSet) {
                ResultSet res = statement.getResultSet();
                System.out.println("                    Time" + "|" + path);
                while (res.next()) {
                    long time = Long.parseLong(res.getString("Time"));
                    String dateTime = dateFormat.format(new Date(time));
                    System.out.println(dateTime + " | " + res.getString(path));
                }
                res.close();
            }
        }
        finally {
            // 8. Close
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }
}


```

# Python connection
## introduction
This is an example of how to connect to IoTDB with python, using the thrift rpc interfaces. Things 
are almost the same on Windows or Linux, but pay attention to the difference like path separator.

## Prerequisites
python3.7 or later is preferred.

You have to install Thrift (0.11.0 or later) to compile our thrift file into python code. Below is the official
tutorial of installation, eventually, you should have a thrift executable.
```
http://thrift.apache.org/docs/install/
```

## Compile the thrift library
If you have added Thrift executable into your path, you may just run `client-py/compile.sh` or
 `client-py/compile.bat`, or you will have to modify it to set variable `THRIFT_EXE` to point to
your executable. This will generate thrift sources under folder `target`, you can add it to your
`PYTHONPATH` so that you would be able to use the library in your code. Notice that the scripts
locate the thrift source file by relative path, so if you move the scripts else where, they are
no longer valid.

Optionally, if you know the basic usage of thrift, you can only download the thrift source file in
`service-rpc\src\main\thrift\rpc.thrift`, and simply use `thrift -gen py -out ./target rpc.thrift` 
to generate the python library.

## Example
We provided an example of how to use the thrift library to connect to IoTDB below, please read it
carefully before you write your own code.
```python
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
    values = bytearray()
    times = bytearray()
    deviceId = "root.group1"
    measurements = ["s1", "s2", "s3"]
    dataSize = 3
    dataTypes = [TSDataType['INT64'], TSDataType['INT64'], TSDataType['INT64']]
    # the first 3 belong to 's1', the mid 3 belong to 's2', the last 3 belong to 's3'
    values.extend(struct.pack('>qqqqqqqqq', 2, 3, 4, 22, 33, 44, 222, 333, 444))
    times.extend(struct.pack('>qqq', 2, 3, 4))
    resp = client.insertBatch(TSBatchInsertionReq(deviceId, measurements, values, times,
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

```