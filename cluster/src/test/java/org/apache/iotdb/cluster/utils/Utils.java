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
package org.apache.iotdb.cluster.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Utils {
  public static String getCurrentPath(String... command) throws IOException {
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.redirectErrorStream(true);
    Process p = builder.start();
    BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String path = r.readLine();
    return path;
  }


  public static void insertData(Connection connection, String[] createSQLs, String[] insertSQLs) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String createSql : createSQLs) {
        statement.execute(createSql);
      }
      for (String insertSql : insertSQLs) {
        statement.execute(insertSql);
      }
    }
  }

  public static void insertBatchData(Connection connection, String[] createSQLs, String[] insertSQLs) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String createSql : createSQLs) {
        statement.addBatch(createSql);
      }
      for (String insertSql : insertSQLs) {
        statement.addBatch(insertSql);
      }
      statement.executeBatch();
    }
  }
}
