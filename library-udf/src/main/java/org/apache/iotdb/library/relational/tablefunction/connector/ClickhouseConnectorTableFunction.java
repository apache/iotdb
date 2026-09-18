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

package org.apache.iotdb.library.relational.tablefunction.connector;

import org.apache.iotdb.library.i18n.LibraryUdfMessages;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.library.relational.tablefunction.connector.JDBCConnectionPool.translateJDBCTypeToUDFType;

public class ClickhouseConnectorTableFunction extends BaseJDBCConnectorTableFunction {

  private static final String DEFAULT_URL = "jdbc:ch://localhost:8123";
  private static final String DEFAULT_USERNAME = "default";
  private static final String DEFAULT_PASSWORD = "";
  private static final String CLICKHOUSE = "CLICKHOUSE";

  @Override
  String getDefaultUrl() {
    return DEFAULT_URL;
  }

  @Override
  String getDefaultUser() {
    return DEFAULT_USERNAME;
  }

  @Override
  String getDefaultPassword() {
    return DEFAULT_PASSWORD;
  }

  @Override
  int[] buildResultHeaders(
      DescribedSchema.Builder schemaBuilder,
      String sql,
      String url,
      String userName,
      String password) {
    int[] types;
    try (Connection connection =
            JDBCConnectionPool.getConnection(getDriverClassName(), url, userName, password);
        Statement statement = connection.createStatement()) {
      statement.setMaxRows(1);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        types = new int[metaData.getColumnCount()];
        for (int i = 1, size = metaData.getColumnCount(); i <= size; i++) {
          int type = metaData.getColumnType(i);
          schemaBuilder.addField(metaData.getColumnLabel(i), translateJDBCTypeToUDFType(type));
          types[i - 1] = type;
        }
        return types;
      }
    } catch (SQLException e) {
      throw new UDFException(
          String.format(
              LibraryUdfMessages.EXCEPTION_FAILED_TO_READ_JDBC_RESULT_METADATA_ARG_16D4E50C,
              e.getMessage()),
          e);
    }
  }

  @Override
  String getDriverClassName() {
    return "com.clickhouse.jdbc.ClickHouseDriver";
  }

  @Override
  String getDBName() {
    return CLICKHOUSE;
  }
}
