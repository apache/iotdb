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
import org.apache.calcite.schema.SchemaPlus;

public class IoTDBClient {

  public static void main(String[] args) {
    try {
      Class.forName("org.apache.calcite.jdbc.Driver");
/*      Properties info = new Properties();
      String jsonFile = Sources.of(IoTDBClient.class.getResource("/model.json")).file().getAbsolutePath();*/
      Connection connection = DriverManager.getConnection("jdbc:calcite:");
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      rootSchema.add("IoTDBSchema",
          new IoTDBSchema("127.0.0.1", 6667, "root", "root", rootSchema, "IoTDBSchema"));
      calciteConnection.setSchema("IoTDBSchema");
      Statement statement = calciteConnection.createStatement();
      String sql = "SELECT \"temperature\" FROM \"root.ln\" WHERE \"temperature\" > 10";
      ResultSet resultSet = statement.executeQuery(sql);

      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        System.out.print(metaData.getColumnLabel(i) + "\t| ");
      }
      System.out.println();

      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; i++) {
          System.out.print(resultSet.getObject(i) + "\t| ");
        }
        System.out.println();
      }

      resultSet.close();
      statement.close();
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return;
  }
}
