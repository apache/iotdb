/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * The class is to show how to write and read date from IoTDB through JDBC
 */
package org.apache.iotdb.jdbc.jdbcDemo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SendDataToIotdb {

  Connection connection = null;
  Statement statement = null;
  ResultSet resultSet = null;

  public static void main(String[] args) throws Exception {
    SendDataToIotdb sendDataToIotdb = new SendDataToIotdb();
    sendDataToIotdb.connectToIotdb();
    sendDataToIotdb.writeData("sensor1,2017/10/24 19:33:00,40 41 42");
    sendDataToIotdb.writeData("sensor2,2017/10/24 19:33:00,140 141 142");
    sendDataToIotdb.writeData("sensor3,2017/10/24 19:33:00,240 241 242");
    sendDataToIotdb.writeData("sensor4,2017/10/24 19:33:00,340 341 342");
    sendDataToIotdb.readData("root.vehicle.sensor.sensor4");
  }

  public void connectToIotdb() throws Exception {
    // 1. load JDBC driver of IoTDB
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    // 2. DriverManager connect to IoTDB
    connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");

    statement = connection.createStatement();
  }

  public void writeData(String out) throws Exception { // write data to IoTDB

    String item[] = out.split(",");

    // get table structure information from IoTDB-JDBC
    DatabaseMetaData databaseMetaData = connection.getMetaData();

    // String path is the path to insert
    String path = "root.vehicle.sensor." + item[0];

    // get path set iterator
    resultSet = databaseMetaData.getColumns(null, null, path, null);

    // if path set iterator is null, then create path
    if (!resultSet.next()) {
      String epl = "CREATE TIMESERIES " + path + " WITH DATATYPE=TEXT, ENCODING=PLAIN";
      statement.execute(epl);
    }
    // insert data to IoTDB
    String template = "INSERT INTO root.vehicle.sensor(timestamp,%s) VALUES (%s,'%s')";
    String epl = String.format(template, item[0], item[1], item[2]);
    statement.execute(epl);
  }

  public void readData(String path) throws Exception { // read data from IoTDB

    String sql = "select * from root.vehicle";
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
    }
  }
}
