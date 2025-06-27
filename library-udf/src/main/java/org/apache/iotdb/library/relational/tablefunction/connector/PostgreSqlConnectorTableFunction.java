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

public class PostgreSqlConnectorTableFunction extends BaseJDBCConnectorTableFunction {

  private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432";
  private static final String DEFAULT_USERNAME = "postgres";
  private static final String DEFAULT_PASSWORD = "";
  private static final String PG = "PostgreSQL";

  @Override
  String getDefaultUrl() {
    return DEFAULT_URL;
  }

  @Override
  String getDefaultUser() {
    return DEFAULT_PASSWORD;
  }

  @Override
  String getDefaultPassword() {
    return DEFAULT_USERNAME;
  }

  @Override
  BaseJDBCConnectorTableFunction.JDBCProcessor getProcessor(
      BaseJDBCConnectorTableFunction.BaseJDBCConnectorTableFunctionHandle tableFunctionHandle) {
    return new PostgreSqlProcessor(tableFunctionHandle);
  }

  private static class PostgreSqlProcessor extends JDBCProcessor {

    PostgreSqlProcessor(
        BaseJDBCConnectorTableFunction.BaseJDBCConnectorTableFunctionHandle handle) {
      super(handle);
    }

    @Override
    String getDBName() {
      return PG;
    }
  }
}
