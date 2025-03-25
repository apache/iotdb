/*
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

package org.apache.iotdb;

import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class TableModelJDBCExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableModelJDBCExample.class);

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");

    // don't specify database in url
    try (Connection connection =
            DriverManager.getConnection(
                "jdbc:iotdb://127.0.0.1:6667?sql_dialect=table", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("use test_g_0");
      // show tables from current database
      long startTime = System.currentTimeMillis();
      try (ResultSet resultSet = statement.executeQuery("SELECT * from table_0")){
//      try (ResultSet resultSet = statement.executeQuery("SELECT device_id, window_start, window_end, s_0" +
//              " FROM TABLE(SESSION(" +
//              "     DATA => TABLE(table_0) PARTITION BY device_id," +
//              "     TIMECOL => 'time'," +
//              "     GAP => 1500ms" +
//              " ))")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        System.out.println(metaData.getColumnCount());
        long count = 0;
        while (resultSet.next()) {
            count++;
            if(count % 10000 == 0){
                System.out.println(count);
            }
//          System.out.println();
//          System.out.println(resultSet.getString(1) + ", " + resultSet.getInt(2));
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time: " + (endTime - startTime) + "ms");
      }



    } catch (IoTDBSQLException e) {
      LOGGER.error("IoTDB Jdbc example error", e);
    }
  }
}
