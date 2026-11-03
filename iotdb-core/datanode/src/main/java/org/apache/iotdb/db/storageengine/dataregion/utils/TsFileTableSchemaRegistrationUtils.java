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

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.iotdb.commons.schema.table.TsFileTableSchemaUtil;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

/**
 * Ensures that a table-model TsFile retains a TableSchema for every table that has data in it.
 *
 * <p>For a table-model TsFile, a non-empty table schema map is the marker that distinguishes it
 * from a tree-model TsFile. This class deliberately only changes map membership: it never adds,
 * removes, or rewrites columns in an existing TableSchema. In particular, an empty schema (for
 * example, one produced from a time-only table by the existing schema converter) is still written
 * as-is so that the table remains identifiable.
 */
public final class TsFileTableSchemaRegistrationUtils {

  private TsFileTableSchemaRegistrationUtils() {}

  /**
   * Registers the missing TableSchema for every table that already has chunks in the writer.
   *
   * <p>This is deliberately called both when a normal flush ends a file and when an unsealed file
   * is recovered: in both cases chunks may already be present while the schema map is empty or
   * incomplete (for example because a table was written before the table schema was registered, or
   * because the WAL entry for an already-flushed chunk has been removed).
   */
  public static void ensureTableSchemasForExistingChunks(
      final TsFileIOWriter writer, final String databaseName) {
    if (writer == null || !PathUtils.isTableModelDatabase(databaseName)) {
      return;
    }
    for (final IDeviceID deviceId : writer.getDeviceChunkMetadataMap().keySet()) {
      registerTableSchemaIfAbsent(writer, databaseName, deviceId.getTableName());
    }
  }

  /**
   * Registers the TableSchema for {@code tableName} if it is absent. A table that has already been
   * dropped still gets an empty schema entry so that the chunks already present in the file do not
   * silently turn it into a tree-model TsFile.
   */
  public static void registerTableSchemaIfAbsent(
      final TsFileIOWriter writer, final String databaseName, final String tableName) {
    if (writer == null || tableName == null) {
      return;
    }
    if (writer.getSchema().getTableSchemaMap().containsKey(tableName)) {
      return;
    }

    final TsTable table = DataNodeTableCache.getInstance().getTable(databaseName, tableName, false);
    writer
        .getSchema()
        .registerTableSchema(
            table == null
                ? new TableSchema(tableName)
                : TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table));
  }
}
