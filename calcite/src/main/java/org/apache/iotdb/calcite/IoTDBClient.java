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
package org.apache.iotdb.calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.calcite.jdbc.CalciteConnection;

public class IoTDBClient {

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Connection connection = DriverManager
        .getConnection("jdbc:calcite:model=./calcite/src/test/resources/model.json");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    /*SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("IoTDBSchema",
        new IoTDBSchema("127.0.0.1", 6667, "root", "root", rootSchema, "IoTDBSchema"));
    calciteConnection.setSchema("IoTDBSchema");*/

    Statement statement = calciteConnection.createStatement();
/*    String sql = "SELECT \"root.ln\".\"device\", \"root.ln\".\"temperature\", \"root.sg\".\"device\", \"root.sg\".\"temperature\" "
        + "FROM \"root.ln\" "
        + "JOIN \"root.sg\" ON \"root.sg\".\"time\" = \"root.ln\".\"time\"";*/
    String sql = "SELECT \"root.ln\".\"device\", \"root.ln\".\"temperature\", \"root.sg\".\"device\", \"root.sg\".\"temperature\" "
        + "FROM \"root.ln\", \"root.sg\" "
        + "WHERE \"root.ln\".\"time\" = \"root.sg\".\"time\"";

    IoTDBClient ioTDBClient = new IoTDBClient();
    ioTDBClient.query(statement, sql);

    statement.close();
    connection.close();
    return;
  }

  public void query(Statement statement, String sql) throws SQLException {
    ResultSet resultSet = statement.executeQuery(sql);

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

    resultSet.close();
  }
}
