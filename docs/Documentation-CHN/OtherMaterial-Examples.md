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

## C++ API Example

``` C++
 
#include "IOTDBSession.h"

using namespace std;
int main()
{
    Session *session = new Session("127.0.0.1", 6667, "root", "root");
    
    session->open();
    
    session->setStorageGroup("root.sg1");
    
    session->createTimeseries("root.sg1.d1.s1", INT64, RLE, SNAPPY);
    session->createTimeseries("root.sg1.d1.s2", INT64, RLE, SNAPPY);
    session->createTimeseries("root.sg1.d1.s3", INT64, RLE, SNAPPY);
    
    string deviceId = "root.sg1.d1";
    vector<string> measurements;
    measurements.push_back("s1");
    measurements.push_back("s2");
    measurements.push_back("s3");
    for (int time = 0; time < 100; time++) 
    {

        vector<string> values;
        values.push_back("1");
        values.push_back("2");
        values.push_back("3");
        session->insert(deviceId, time, measurements, values);
    }
    
    vector<string> del;
    del.push_back("root.sg1.d1.s1");
    session->deleteData(del, 99);
    
    
    vector<string> paths;
    paths.push_back("root.sg1.d1.s1");
    paths.push_back("root.sg1.d1.s2");
    paths.push_back("root.sg1.d1.s3");
    session->deleteTimeseries(paths);
    
    session->deleteStorageGroup("root.sg1");
    
    session->close();
}



```
