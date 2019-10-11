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

# IoTDB实例

这些示例快速概述了IoTDB JDBC。IoTDB为用户提供了标准的JDBC与IoTDB交互，其他语言版本很快就会融合。

要使用IoTDB，您需要为您的时间序列设置存储组（有关存储组的详细概念，请查看我们的文档）。然后您需要根据数据类型、名称等创建特定的时间序列（存储组的详细概念，请查看我们的文档），之后可以插入和查询数据。在本页中，我们将展示使用iotdb jdbc的基本示例。
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
# 使用Python进行连接
## 介绍
以下例子展示了如何使用Thrift编译出Python的库，并调用该库连接IoTDB并进行一些基本操作。在Windows和Linux上的
使用大同小异，但请注意二者在路径等方面的细微区别。

## Prerequisites
建议使用python3.7或更高级的版本。

您需要预先安装Thrift才能将我们的Thrift源文件编译为Python的库代码。下面是Thrift的官方安装教程，最终，您需要
得到一个Thrift的二进制可执行文件。
```
http://thrift.apache.org/docs/install/
```

## Compile the thrift library
如果您已经将Thrift的可执行文件放到了您的PATH环境变量的目录下，您可以直接运行脚本`client-py/compile.sh`或
`client-py/compile.bat`，否则您需要修改脚本中的变量`THRIFT_EXE`来指向您的Thrift可执行文件。这会在脚本文件
的同一目录下生成`target`文件夹，该文件夹即包含了IoTDB的Python版本的Thrift库，当您将该文件夹加入到您的`PYTHONPATH`
环境变量里后，您就可以在自己的代码中引用该库。请注意，上述脚本通过相对路径寻找我们的Thrift的源文件，如果您改变
脚本所在位置，脚本可能失效。

或者，如果您对Thrift的基本用法有所了解，您就不需要下载整个项目，您可以仅下载我们的Thrift源文件`service-rpc\src\main\thrift\rpc.thrift`，
并使用命令`thrift -gen py -out ./target rpc.thrift`来编译Thrift库。

## Example
下面是一段使用生成的Thrift库连接IoTDB的示例代码，请在编写您自己的代码之前仔细地阅读。
```python

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