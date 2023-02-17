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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SyntaxConventionRelatedExample {
  /**
   * if you want to create a time series named root.sg1.select, a possible SQL statement would be
   * like: create timeseries root.sg1.select with datatype=FLOAT, encoding=RLE As described before,
   * when using session API, path is represented using String. The path should be written as
   * "root.sg1.select".
   */
  private static final String ROOT_SG1_KEYWORD_EXAMPLE = "root.sg1.select";

  /**
   * if you want to create a time series named root.sg1.111, a possible SQL statement would be like:
   * create timeseries root.sg1.`111` with datatype=FLOAT, encoding=RLE The path should be written
   * as "root.sg1.`111`".
   */
  private static final String ROOT_SG1_DIGITS_EXAMPLE = "root.sg1.`111`";

  /**
   * if you want to create a time series named root.sg1.`a"b'c``, a possible SQL statement would be
   * like: create timeseries root.sg1.`a"b'c``` with datatype=FLOAT, encoding=RLE The path should be
   * written as "root.sg1.`a"b`c```".
   */
  private static final String ROOT_SG1_SPECIAL_CHARACTER_EXAMPLE = "root.sg1.`a\"b'c```";

  /**
   * if you want to create a time series named root.sg1.a, a possible SQL statement would be like:
   * create timeseries root.sg1.a with datatype=FLOAT, encoding=RLE The path should be written as
   * "root.sg1.a".
   */
  private static final String ROOT_SG1_NORMAL_NODE_EXAMPLE = "root.sg1.a";

  private static final String DEVICE = "root.sg1";

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try (Connection connection =
            DriverManager.getConnection(
                "jdbc:iotdb://127.0.0.1:6667?version=V_1_0", "root", "root");
        Statement statement = connection.createStatement()) {

      // set JDBC fetchSize
      statement.setFetchSize(10000);

      // create time series
      try {
        statement.execute(String.format("CREATE DATABASE %s", DEVICE));
        statement.execute(
            String.format(
                "CREATE TIMESERIES %s WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY",
                ROOT_SG1_DIGITS_EXAMPLE));
        statement.execute(
            String.format(
                "CREATE TIMESERIES %s WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY",
                ROOT_SG1_KEYWORD_EXAMPLE));
        statement.execute(
            String.format(
                "CREATE TIMESERIES %s WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY",
                ROOT_SG1_NORMAL_NODE_EXAMPLE));
        statement.execute(
            String.format(
                "CREATE TIMESERIES %s WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY",
                ROOT_SG1_SPECIAL_CHARACTER_EXAMPLE));
      } catch (IoTDBSQLException e) {
        System.out.println(e.getMessage());
      }

      // show timeseries
      ResultSet resultSet = statement.executeQuery("show timeseries root.sg1.*");
      List<String> timeseriesList = new ArrayList<>();
      while (resultSet.next()) {
        timeseriesList.add(resultSet.getString("Timeseries"));
      }
      for (String path : timeseriesList) {
        for (int i = 0; i <= 10; i++) {
          statement.addBatch(prepareInsertStatement(i, path));
        }
      }
      statement.executeBatch();
      statement.clearBatch();

      resultSet = statement.executeQuery("select ** from root.sg1 where time <= 10");
      outputResult(resultSet);
      for (String path : timeseriesList) {
        // For example, for timeseires root.sg1.`111`, sensor is 111, as described in syntax
        // convention, it should be written as `111` in SQL
        // in resultSet of "show timeseries", result is root.sg1.`111`, which means you need not to
        // worry about dealing with backquotes yourself.
        resultSet =
            statement.executeQuery(String.format("select %s from root.sg1", removeDevice(path)));
        outputResult(resultSet);
      }
    } catch (IoTDBSQLException e) {
      System.out.println(e.getMessage());
    }
  }

  private static void outputResult(ResultSet resultSet) throws SQLException {
    if (resultSet != null) {
      System.out.println("--------------------------");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        System.out.print(metaData.getColumnLabel(i + 1) + " ");
      }
      System.out.println();
      while (resultSet.next()) {
        for (int i = 1; ; i++) {
          System.out.print(resultSet.getString(i));
          if (i < columnCount) {
            System.out.print(", ");
          } else {
            System.out.println();
            break;
          }
        }
      }
      System.out.println("--------------------------\n");
    }
  }

  private static String prepareInsertStatement(int time, String path) {
    // remove device root.sg1
    path = removeDevice(path);
    return String.format(
        "insert into root.sg1(timestamp, %s) values(" + time + "," + 1 + ")", path);
  }

  private static String removeDevice(String path) {
    return path.substring(DEVICE.length() + 1);
  }
}
