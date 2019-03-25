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
