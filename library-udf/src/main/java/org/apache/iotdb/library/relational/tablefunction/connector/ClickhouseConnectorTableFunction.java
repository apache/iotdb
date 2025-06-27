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

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.library.relational.tablefunction.connector.JDBCConnectionPool.translateJDBCTypeToUDFType;

public class ClickhouseConnectorTableFunction extends BaseJDBCConnectorTableFunction {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ClickhouseConnectorTableFunction.class);

  static {
    try {
      Class.forName("com.clickhouse.jdbc.ClickHouseDriver").newInstance();
    } catch (Exception e) {
      LOGGER.warn("Failed to initialize clickhouse JDBC driver", e);
    }
  }

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
    if (!sql.contains("LIMIT")) {
      sql += " LIMIT 1";
    }
    int[] types;
    try (Connection connection = JDBCConnectionPool.getConnection(url, userName, password);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      types = new int[metaData.getColumnCount()];
      for (int i = 1, size = metaData.getColumnCount(); i <= size; i++) {
        int type = metaData.getColumnType(i);
        schemaBuilder.addField(metaData.getColumnName(i), translateJDBCTypeToUDFType(type));
        types[i - 1] = type;
      }
      return types;
    } catch (SQLException e) {
      throw new UDFException(String.format("Get ResultSetMetaData failed. %s", e.getMessage()), e);
    }
  }

  @Override
  BaseJDBCConnectorTableFunction.JDBCProcessor getProcessor(
      BaseJDBCConnectorTableFunction.BaseJDBCConnectorTableFunctionHandle tableFunctionHandle) {
    return new ClickhouseProcessor(tableFunctionHandle);
  }

  private static class ClickhouseProcessor extends BaseJDBCConnectorTableFunction.JDBCProcessor {

    ClickhouseProcessor(
        BaseJDBCConnectorTableFunction.BaseJDBCConnectorTableFunctionHandle handle) {
      super(handle);
    }

    @Override
    String getDBName() {
      return CLICKHOUSE;
    }
  }
}
