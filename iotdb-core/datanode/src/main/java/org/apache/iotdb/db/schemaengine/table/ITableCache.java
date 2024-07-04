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

package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;

import java.util.List;

public interface ITableCache {

  void init(byte[] tableInitializationBytes);

  void preCreateTable(String database, TsTable table);

  void rollbackCreateTable(String database, String tableName);

  void commitCreateTable(String database, String tableName);

  void preAddTableColumn(
      String database, String tableName, List<TsTableColumnSchema> columnSchemaList);

  void commitAddTableColumn(
      String database, String tableName, List<TsTableColumnSchema> columnSchemaList);

  void rollbackAddColumn(
      String database, String tableName, List<TsTableColumnSchema> columnSchemaList);

  /**
   * @param database shouldn't start with `root.`
   */
  void invalid(String database);
}
