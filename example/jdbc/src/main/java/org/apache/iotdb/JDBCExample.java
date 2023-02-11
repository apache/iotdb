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

public class JDBCExample {
  private static final String CREATE_TEMPLATE_SQL =
      "CREATE TIMESERIES root.vehicle.%s.%s WITH DATATYPE=%s, ENCODING=%s, 'MAX_POINT_NUMBER'='%d'";
  private static final String INSERT_TEMPLATE_SQL =
      "insert into root.vehicle.%s(timestamp,%s) values(%d,%s)";
  private static List<String> sqls = new ArrayList<>();
  private static final int TIMESTAMP = 10;
  private static final String VALUE = "1.2345678901";
  private static final float DELTA_FLOAT = 0.0000001f;
  private static final double DELTA_DOUBLE = 0.0000001d;

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try (Connection connection =
            DriverManager.getConnection(
                "jdbc:iotdb://127.0.0.1:6667?version=V_1_0", "root", "root");
        Statement statement = connection.createStatement()) {

      sqls.add("CREATE DATABASE root.vehicle.f0");
      sqls.add("CREATE DATABASE root.vehicle.d0");
      for (int i = 0; i < 10; i++) {
        sqls.add(String.format(CREATE_TEMPLATE_SQL, "f0", "s" + i + "rle", "FLOAT", "RLE", i));
        sqls.add(String.format(CREATE_TEMPLATE_SQL, "f0", "s" + i + "2f", "FLOAT", "TS_2DIFF", i));
        sqls.add(String.format(CREATE_TEMPLATE_SQL, "d0", "s" + i + "rle", "DOUBLE", "RLE", i));
        sqls.add(String.format(CREATE_TEMPLATE_SQL, "d0", "s" + i + "2f", "DOUBLE", "TS_2DIFF", i));
      }
      for (int i = 0; i < 10; i++) {
        sqls.add(String.format(INSERT_TEMPLATE_SQL, "f0", "s" + i + "rle", TIMESTAMP, VALUE));
        sqls.add(String.format(INSERT_TEMPLATE_SQL, "f0", "s" + i + "2f", TIMESTAMP, VALUE));
        sqls.add(String.format(INSERT_TEMPLATE_SQL, "d0", "s" + i + "rle", TIMESTAMP, VALUE));
        sqls.add(String.format(INSERT_TEMPLATE_SQL, "d0", "s" + i + "2f", TIMESTAMP, VALUE));
      }

      for (String sql : sqls) {
        statement.execute(sql);
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

  private static String prepareInsertStatment(int time) {
    return "insert into root.sg1.d1(timestamp, s1, s2, s3) values("
        + time
        + ","
        + 1
        + ","
        + 1
        + ","
        + 1
        + ")";
  }
}
