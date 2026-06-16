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

package org.apache.iotdb.commons.schema.table;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.SchemaMessages;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class WritableView extends TsTable {
  public static final String SCHEMA_CASCADE = "schema_cascade";
  // Transient marker used only by ALTER VIEW/TABLE ADD COLUMN requests to keep writable-view
  // source-column syntax separate from tree-view column syntax.
  public static final String ADD_COLUMN_SOURCE_SYNTAX = "__writable_view_add_column_source_syntax";

  static final int WRITABLE_VIEW = -2;

  // Currently not used, used for potential future development
  private final String sourceTableDatabase;
  private final String sourceTableName;
  private boolean isSchemaCascade;
  private volatile Map<String, String> viewColumnToSourceColumnMap;

  public WritableView(
      final String viewName,
      final String sourceTableDatabase,
      final String sourceTableName,
      final boolean isSchemaCascade) {
    super(viewName);
    this.sourceTableDatabase = sourceTableDatabase;
    this.sourceTableName = sourceTableName;
    this.isSchemaCascade = isSchemaCascade;
  }

  public WritableView(final WritableView origin) {
    super(origin);
    this.sourceTableDatabase = origin.sourceTableDatabase;
    this.sourceTableName = origin.sourceTableName;
    this.isSchemaCascade = origin.isSchemaCascade;
    final Map<String, String> originColumnMap = origin.viewColumnToSourceColumnMap;
    this.viewColumnToSourceColumnMap =
        Objects.nonNull(originColumnMap) ? new LinkedHashMap<>(originColumnMap) : null;
  }

  public static boolean parseSchemaCascade(final String value) {
    if (Objects.isNull(value)) {
      return true;
    }
    if (Boolean.TRUE.toString().equalsIgnoreCase(value)) {
      return true;
    }
    if (Boolean.FALSE.toString().equalsIgnoreCase(value)) {
      return false;
    }
    throw new SemanticException(
        String.format(SchemaMessages.SCHEMA_CASCADE_VALUE_MUST_BE_BOOLEAN, value));
  }

  @Override
  public WritableView clone() {
    return new WritableView(this);
  }

  public void setSchemaCascade(final boolean schemaCascade) {
    executeWrite(() -> isSchemaCascade = schemaCascade);
  }

  public boolean isSchemaCascade() {
    return isSchemaCascade;
  }

  public void setViewColumnToSourceColumnMap(
      final Map<String, String> viewColumnToSourceColumnMap) {
    executeWrite(() -> setViewColumnToSourceColumnMapInternal(viewColumnToSourceColumnMap));
  }

  public Map<String, String> getViewColumnToSourceColumnMap() {
    final Map<String, String> columnMap = viewColumnToSourceColumnMap;
    return Objects.nonNull(columnMap)
        ? Collections.unmodifiableMap(new LinkedHashMap<>(columnMap))
        : null;
  }

  public boolean hasViewColumnSourceColumnMap() {
    return Objects.nonNull(viewColumnToSourceColumnMap);
  }

  public void putViewColumnSourceColumnMapping(
      final String viewColumnName, final String sourceColumnName) {
    executeWrite(
        () -> {
          final Map<String, String> updatedColumnMap = copyViewColumnToSourceColumnMapOrEmpty();
          updatedColumnMap.put(viewColumnName, sourceColumnName);
          viewColumnToSourceColumnMap = updatedColumnMap;
        });
  }

  public void putAllViewColumnSourceColumnMappings(
      final Map<String, String> viewColumnToSourceColumnMappings) {
    if (Objects.isNull(viewColumnToSourceColumnMappings)
        || viewColumnToSourceColumnMappings.isEmpty()) {
      return;
    }
    executeWrite(
        () -> {
          final Map<String, String> updatedColumnMap = copyViewColumnToSourceColumnMapOrEmpty();
          updatedColumnMap.putAll(viewColumnToSourceColumnMappings);
          viewColumnToSourceColumnMap = updatedColumnMap;
        });
  }

  public void removeViewColumnSourceColumnMapping(final String viewColumnName) {
    executeWrite(
        () -> {
          if (Objects.nonNull(viewColumnToSourceColumnMap)) {
            final Map<String, String> updatedColumnMap = copyViewColumnToSourceColumnMapOrEmpty();
            updatedColumnMap.remove(viewColumnName);
            viewColumnToSourceColumnMap = updatedColumnMap;
          }
        });
  }

  public String getOriginalColumnName(final String columnName) {
    final TsTableColumnSchema columnSchema = getColumnSchema(columnName);
    final Map<String, String> columnMap = viewColumnToSourceColumnMap;
    if (Objects.isNull(columnMap)) {
      return Objects.nonNull(columnSchema)
          ? ViewColumnSchemaUtils.getSourceName(columnSchema)
          : columnName;
    }
    return Objects.nonNull(columnSchema)
        ? columnMap.getOrDefault(columnName, ViewColumnSchemaUtils.getSourceName(columnSchema))
        : columnMap.getOrDefault(columnName, columnName);
  }

  public String getMappedSourceColumnName(final String columnName) {
    final Map<String, String> columnMap = viewColumnToSourceColumnMap;
    return Objects.nonNull(columnMap) ? columnMap.get(columnName) : null;
  }

  public boolean hasViewColumnSourceColumnMapping(final String columnName) {
    final Map<String, String> columnMap = viewColumnToSourceColumnMap;
    return Objects.nonNull(columnMap) && columnMap.containsKey(columnName);
  }

  public Optional<String> getMappedSourceColumnNameIfPresent(final String columnName) {
    return Optional.ofNullable(getMappedSourceColumnName(columnName));
  }

  @Override
  public void addColumnSchema(final TsTableColumnSchema columnSchema) {
    executeWrite(
        () -> {
          addColumnSchemaInternal(columnSchema);
          putViewColumnSourceColumnMappingIfAbsent(
              columnSchema.getColumnName(), ViewColumnSchemaUtils.getSourceName(columnSchema));
        });
  }

  @Override
  public void renameColumnSchema(final String oldName, final String newName) {
    executeWrite(
        () -> {
          renameColumnSchemaInternal(oldName, newName);
          if (hasViewColumnSourceColumnMapping(oldName) && !Objects.equals(oldName, newName)) {
            final Map<String, String> updatedColumnMap = copyViewColumnToSourceColumnMapOrEmpty();
            updatedColumnMap.put(newName, updatedColumnMap.remove(oldName));
            viewColumnToSourceColumnMap = updatedColumnMap;
          }
        });
  }

  @Override
  public void removeColumnSchema(final String columnName) {
    executeWrite(
        () -> {
          removeColumnSchemaInternal(columnName, true);
          if (hasViewColumnSourceColumnMap()) {
            final Map<String, String> updatedColumnMap = copyViewColumnToSourceColumnMapOrEmpty();
            updatedColumnMap.remove(columnName);
            viewColumnToSourceColumnMap = updatedColumnMap;
          }
        });
  }

  public String getSourceTableDatabase() {
    return sourceTableDatabase;
  }

  public String getSourceTableName() {
    return sourceTableName;
  }

  @Override
  public int getType() {
    return TableType.WRITABLE_VIEW.ordinal();
  }

  @Override
  public void serialize(final OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tableName, stream);

    ReadWriteIOUtils.write(WRITABLE_VIEW, stream);
    ReadWriteIOUtils.write(sourceTableDatabase, stream);
    ReadWriteIOUtils.write(sourceTableName, stream);
    ReadWriteIOUtils.write(isSchemaCascade, stream);
    final Map<String, String> columnMap = viewColumnToSourceColumnMap;
    ReadWriteIOUtils.write(columnMap, stream);

    ReadWriteIOUtils.write(columnSchemaMap.size(), stream);
    for (final TsTableColumnSchema columnSchema : columnSchemaMap.values()) {
      TsTableColumnSchemaUtil.serialize(columnSchema, stream);
    }
    ReadWriteIOUtils.write(props, stream);
  }

  public static WritableView deserialize(final String tableName, final InputStream inputStream)
      throws IOException {
    final WritableView view =
        new WritableView(
            tableName,
            ReadWriteIOUtils.readString(inputStream),
            ReadWriteIOUtils.readString(inputStream),
            ReadWriteIOUtils.readBool(inputStream));
    view.setViewColumnToSourceColumnMapInternal(deserializeOrderedMap(inputStream));

    final int columnNum = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < columnNum; i++) {
      view.addColumnSchema(TsTableColumnSchemaUtil.deserialize(inputStream));
    }
    view.props = ReadWriteIOUtils.readMap(inputStream);
    return view;
  }

  public static WritableView deserialize(final String tableName, final ByteBuffer byteBuffer) {
    final WritableView view =
        new WritableView(
            tableName,
            ReadWriteIOUtils.readString(byteBuffer),
            ReadWriteIOUtils.readString(byteBuffer),
            ReadWriteIOUtils.readBool(byteBuffer));
    view.setViewColumnToSourceColumnMapInternal(deserializeOrderedMap(byteBuffer));

    final int columnNum = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < columnNum; i++) {
      view.addColumnSchema(TsTableColumnSchemaUtil.deserialize(byteBuffer));
    }
    view.props = ReadWriteIOUtils.readMap(byteBuffer);
    return view;
  }

  @Override
  public String toString() {
    return "WritableView{"
        + "tableName='"
        + tableName
        + '\''
        + ", columnSchemaMap="
        + columnSchemaMap
        + ", props="
        + props
        + ", sourceTableDatabase='"
        + sourceTableDatabase
        + '\''
        + ", sourceTableName='"
        + sourceTableName
        + '\''
        + ", isSchemaCascade="
        + isSchemaCascade
        + ", viewColumnToSourceColumnMap="
        + viewColumnToSourceColumnMap
        + '}';
  }

  private static Map<String, String> deserializeOrderedMap(final InputStream inputStream)
      throws IOException {
    final int size = ReadWriteIOUtils.readInt(inputStream);
    if (size == -1) {
      return null;
    }
    final Map<String, String> orderedMap = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      orderedMap.put(
          ReadWriteIOUtils.readString(inputStream), ReadWriteIOUtils.readString(inputStream));
    }
    return orderedMap;
  }

  private static Map<String, String> deserializeOrderedMap(final ByteBuffer byteBuffer) {
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    if (size == -1) {
      return null;
    }
    final Map<String, String> orderedMap = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      orderedMap.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    return orderedMap;
  }

  private void setViewColumnToSourceColumnMapInternal(
      final Map<String, String> viewColumnToSourceColumnMap) {
    this.viewColumnToSourceColumnMap =
        Objects.nonNull(viewColumnToSourceColumnMap)
            ? new LinkedHashMap<>(viewColumnToSourceColumnMap)
            : null;
  }

  private Map<String, String> copyViewColumnToSourceColumnMapOrEmpty() {
    final Map<String, String> columnMap = viewColumnToSourceColumnMap;
    return Objects.nonNull(columnMap) ? new LinkedHashMap<>(columnMap) : new LinkedHashMap<>();
  }

  private void putViewColumnSourceColumnMappingIfAbsent(
      final String viewColumnName, final String sourceColumnName) {
    final Map<String, String> updatedColumnMap = copyViewColumnToSourceColumnMapOrEmpty();
    updatedColumnMap.putIfAbsent(viewColumnName, sourceColumnName);
    viewColumnToSourceColumnMap = updatedColumnMap;
  }
}
