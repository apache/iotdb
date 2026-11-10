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
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Keeps a table-model TsFile identifiable and self-describing.
 *
 * <p>Every table-model TsFile carries a single {@value #TABLE_MODEL_PROPERTY} file property. It is
 * a constant-time marker that tells readers the file belongs to the table model, so a file whose
 * TableSchema map is empty is not mistaken for a tree-model TsFile.
 *
 * <p>The TableSchema map keeps one entry per table that writes data into the file. This class only
 * changes map membership: it never adds, removes, or rewrites columns in an existing TableSchema.
 * In particular it does not synthesize an entry for a table that no longer exists, the file stays
 * recognizable through the file property instead.
 */
public final class TsFileTableSchemaRegistrationUtils {

  /** File property written by every table-model TsFile; its value is {@code true}. */
  public static final String TABLE_MODEL_PROPERTY = "isTableModel";

  private static final byte[] TABLE_MODEL_PROPERTY_VALUE = "true".getBytes(StandardCharsets.UTF_8);

  private TsFileTableSchemaRegistrationUtils() {}

  /**
   * Records that the file being written belongs to the table model.
   *
   * <p>Call it once per file, when the file is sealed, that is, immediately before {@link
   * TsFileIOWriter#endFile()}. The marker is a single entry of the in-memory file property map and
   * is serialized together with the file metadata by {@code endFile()}, so it costs nothing on the
   * write path and must never be called per record, per tablet or per chunk group.
   */
  public static void markTableModel(final TsFileIOWriter writer) {
    if (writer == null) {
      return;
    }
    writer.addTsFileProperty(TABLE_MODEL_PROPERTY, TABLE_MODEL_PROPERTY_VALUE);
  }

  /**
   * Whether the given file metadata says that the TsFile was written as a table-model file. This
   * does not look at the TableSchema map, so it also recognizes a table-model file whose schema map
   * is empty.
   */
  public static boolean isTableModel(final TsFileMetadata metadata) {
    return metadata != null
        && metadata.getTsFileProperties() != null
        && metadata.getTsFileProperties().containsKey(TABLE_MODEL_PROPERTY);
  }

  /**
   * Registers the TableSchema of {@code tableName} when the writer does not have it yet.
   *
   * <p>This is called while an unsealed table-model file is recovered: the TableSchema of a table
   * whose data is replayed from the WAL has to be registered again, otherwise the closed file would
   * carry the chunks of the table but no description of them.
   *
   * <p>A table that no longer exists is skipped instead of being written with an empty schema: the
   * storage path does not describe tables it cannot read, and the file property already tells
   * readers that the file is a table-model one.
   *
   * <p>The caller invokes this only for a table-model DataRegion, so {@code databaseName} is used
   * here solely as a cache key and is never inspected to guess the model. Inside this method the
   * table cache is the only source of truth: a table that no longer exists has no entry and is
   * skipped, and a tree-model database never owns a {@link TsTable} at all.
   */
  public static void registerTableSchemaIfAbsent(
      final TsFileIOWriter writer, final String databaseName, final String tableName) {
    if (Objects.isNull(writer)
        || Objects.isNull(tableName)
        || writer.getSchema().getTableSchemaMap().containsKey(tableName)) {
      return;
    }
    final TsTable table = DataNodeTableCache.getInstance().getTable(databaseName, tableName, false);
    if (Objects.isNull(table)) {
      return;
    }
    writer
        .getSchema()
        .registerTableSchema(TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table));
  }
}
