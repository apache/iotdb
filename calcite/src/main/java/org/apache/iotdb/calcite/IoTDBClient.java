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
    String sql = "SELECT \"temperature\" FROM \"root.ln\" WHERE \"time\" = 100";

    IoTDBClient ioTDBClient = new IoTDBClient();
    ioTDBClient.query(statement, sql);

    statement.close();
    connection.close();
    return;
  }

  public void query(Statement statement, String sql) throws SQLException {
    long startTime = System.currentTimeMillis();
    ResultSet resultSet = statement.executeQuery(sql);
    while(resultSet.next()){
    }
    long endTime = System.currentTimeMillis();
    System.out.println(endTime - startTime);

    resultSet.close();
  }
}
