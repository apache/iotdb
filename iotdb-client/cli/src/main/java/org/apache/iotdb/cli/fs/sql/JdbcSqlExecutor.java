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

package org.apache.iotdb.cli.fs.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JdbcSqlExecutor implements SqlExecutor {

  private final Connection connection;

  public JdbcSqlExecutor(Connection connection) {
    this.connection = connection;
  }

  @Override
  public List<SqlRow> query(String sql) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      return readRows(resultSet);
    }
  }

  @Override
  public List<SqlRow> executeQueryOrUpdate(String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      if (!statement.execute(sql)) {
        return Collections.emptyList();
      }
      try (ResultSet resultSet = statement.getResultSet()) {
        return readRows(resultSet);
      }
    }
  }

  private static List<SqlRow> readRows(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    Map<String, String> dataTypes = new LinkedHashMap<>();
    for (int column = 1; column <= columnCount; column++) {
      dataTypes.put(metaData.getColumnLabel(column), metaData.getColumnTypeName(column));
    }
    List<SqlRow> rows = new ArrayList<>();
    while (resultSet.next()) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int column = 1; column <= columnCount; column++) {
        String name = metaData.getColumnLabel(column);
        if ("TIMESTAMP".equalsIgnoreCase(dataTypes.get(name))) {
          long timestamp = resultSet.getLong(column);
          values.put(name, resultSet.wasNull() ? null : Long.toString(timestamp));
        } else {
          values.put(name, resultSet.getString(column));
        }
      }
      rows.add(new SqlRow(values, dataTypes));
    }
    return rows;
  }

  @Override
  public void execute(String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }
}
