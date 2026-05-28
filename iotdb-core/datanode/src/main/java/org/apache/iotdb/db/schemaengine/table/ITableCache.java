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

import javax.annotation.Nullable;

public interface ITableCache {

  void init(final byte[] tableInitializationBytes);

  void preUpdateTable(final String database1, final TsTable table, final @Nullable String oldName);

  void preUpdateTable(
      final String database1, final String database2, final TsTable table1, final TsTable table2);

  void rollbackUpdateTable(
      final String database, final String tableName, final @Nullable String oldName);

  void rollbackUpdateTable(
      final String database1,
      final String database2,
      final String tableName1,
      final String tableName2);

  boolean commitUpdateTable(
      final String database, final String tableName, final @Nullable String oldName);

  boolean commitUpdateTable(
      final String database1,
      final String database2,
      final String tableName1,
      final String tableName2);

  /**
   * @param database shouldn't start with `root.`
   */
  void invalid(final String database);

  void invalid(final String database, final String tableName);

  void invalid(
      final String database1,
      final String tableName1,
      final String database2,
      final String tableName2);

  void invalid(final String database, final String tableName, final String columnName);

  void invalid(
      final String database1,
      final String tableName1,
      final String columnName1,
      final String database2,
      final String tableName2,
      final String columnName2);
}
