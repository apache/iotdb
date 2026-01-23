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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.commons.conf.IoTDBConstant.TTL_INFINITE;

@ThreadSafe
public class TsTable {

  public static final String TIME_COLUMN_NAME = "time";
  public static final String COMMENT_KEY = "__comment";
  private static final TimeColumnSchema TIME_COLUMN_SCHEMA =
      new TimeColumnSchema(TIME_COLUMN_NAME, TSDataType.TIMESTAMP);

  public static final String TTL_PROPERTY = "ttl";
  public static final Set<String> TABLE_ALLOWED_PROPERTIES = Collections.singleton(TTL_PROPERTY);
  private static final String OBJECT_STRING_ERROR =
      "When there are object fields, the %s %s shall not be '.', '..' or contain './', '.\\'.";
  protected String tableName;

  // Copy-on-Write maps for thread-safe access without read locks
  private volatile Map<String, TsTableColumnSchema> columnSchemaMap = new LinkedHashMap<>();
  private volatile Map<String, Integer> tagColumnIndexMap = new HashMap<>();
  private volatile Map<String, Integer> idColumnIndexMap = new HashMap<>();

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  /**
   * Global sequence generator providing unique, monotonically increasing IDs across all instances.
   * Initialized to -1 to ensure the first ID is 0.
   */
  private static final AtomicLong GLOBAL_SEQUENCE = new AtomicLong(-1);

  private final transient Long creationId = GLOBAL_SEQUENCE.getAndIncrement();
  private final transient AtomicLong instanceVersion = new AtomicLong(0L);

  private final transient AtomicBoolean isNotWrite = new AtomicBoolean(true);
  private final AtomicReference<Pair<Long, List<TsTableColumnSchema>>> tagColumnSchemas =
      new AtomicReference<>();

  private Map<String, String> props = null;

  // Cache, avoid string parsing
  private transient long ttlValue = Long.MIN_VALUE;
  private transient int tagNums = 0;
  private transient int fieldNum = 0;

  public TsTable(final String tableName) {
    this.tableName = tableName;
    columnSchemaMap.put(TIME_COLUMN_NAME, TIME_COLUMN_SCHEMA);
  }

  // This interface is used by InformationSchema table, so time column is not necessary
  public TsTable(String tableName, ImmutableList<TsTableColumnSchema> columnSchemas) {
    this.tableName = tableName;
    columnSchemas.forEach(
        columnSchema -> columnSchemaMap.put(columnSchema.getColumnName(), columnSchema));
  }

  public TsTable(TsTable origin) {
    this.tableName = origin.tableName;
    origin.columnSchemaMap.forEach((col, schema) -> this.columnSchemaMap.put(col, schema.copy()));
    this.idColumnIndexMap.putAll(origin.idColumnIndexMap);
    this.props = origin.props == null ? null : new HashMap<>(origin.props);
    this.ttlValue = origin.ttlValue;
    this.tagNums = origin.tagNums;
    this.fieldNum = origin.fieldNum;
  }

  public String getTableName() {
    return tableName;
  }

  /**
   * Get column schema with optimistic lock for fast reads. This method uses a lock-free fast path
   * when there's no concurrent write operation, significantly improving read performance.
   *
   * @param columnName the column name to query
   * @return the column schema, or null if not found
   */
  public TsTableColumnSchema getColumnSchema(final String columnName) {
    final long versionBefore = instanceVersion.get();
    final TsTableColumnSchema result = columnSchemaMap.get(columnName);
    if (isNotWrite.get() && instanceVersion.get() == versionBefore) {
      return result;
    }

    // Slow path: write in progress or version changed, acquire read lock
    readWriteLock.readLock().lock();
    try {
      return columnSchemaMap.get(columnName);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Execute a write operation with Copy-on-Write support. This method creates new copies of the
   * maps before modification to ensure thread-safe reads without locks.
   *
   * @param writeOperation the write operation to execute on the new map copies
   */
  private void executeWrite(WriteOperation writeOperation) {
    readWriteLock.writeLock().lock();
    isNotWrite.set(false);
    try {
      // Copy-on-Write: create local copies first
      Map<String, TsTableColumnSchema> newColumnSchemaMap = new LinkedHashMap<>(columnSchemaMap);
      Map<String, Integer> newTagColumnIndexMap = new HashMap<>(tagColumnIndexMap);
      Map<String, Integer> newIdColumnIndexMap = new HashMap<>(idColumnIndexMap);

      // Execute write operation on local copies
      writeOperation.execute(newColumnSchemaMap, newTagColumnIndexMap, newIdColumnIndexMap);

      // After write completes, atomically update the class fields
      columnSchemaMap = newColumnSchemaMap;
      tagColumnIndexMap = newTagColumnIndexMap;
      idColumnIndexMap = newIdColumnIndexMap;
    } finally {
      instanceVersion.incrementAndGet();
      isNotWrite.set(true);
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Execute a write operation with a custom columnSchemaMap transformer. This allows transforming
   * the map during copy (e.g., for rename operations) in a single pass.
   *
   * @param columnSchemaMapTransformer transforms columnSchemaMap entries during copy
   * @param writeOperation the write operation to execute on the new map copies
   */
  private void executeWriteWithTransform(
      ColumnSchemaMapTransformer columnSchemaMapTransformer, WriteOperation writeOperation) {
    readWriteLock.writeLock().lock();
    isNotWrite.set(false);
    try {
      // Copy-on-Write with transformation: transform columnSchemaMap in single pass
      Map<String, TsTableColumnSchema> newColumnSchemaMap = new LinkedHashMap<>();
      for (Map.Entry<String, TsTableColumnSchema> entry : columnSchemaMap.entrySet()) {
        columnSchemaMapTransformer.transform(entry.getKey(), entry.getValue(), newColumnSchemaMap);
      }
      Map<String, Integer> newTagColumnIndexMap = new HashMap<>(tagColumnIndexMap);
      Map<String, Integer> newIdColumnIndexMap = new HashMap<>(idColumnIndexMap);

      // Execute write operation on local copies
      writeOperation.execute(newColumnSchemaMap, newTagColumnIndexMap, newIdColumnIndexMap);

      // After write completes, atomically update the class fields
      columnSchemaMap = newColumnSchemaMap;
      tagColumnIndexMap = newTagColumnIndexMap;
      idColumnIndexMap = newIdColumnIndexMap;
    } finally {
      instanceVersion.incrementAndGet();
      isNotWrite.set(true);
      readWriteLock.writeLock().unlock();
    }
  }

  @FunctionalInterface
  private interface WriteOperation {
    void execute(
        Map<String, TsTableColumnSchema> columnSchemaMap,
        Map<String, Integer> tagColumnIndexMap,
        Map<String, Integer> idColumnIndexMap);
  }

  @FunctionalInterface
  private interface ColumnSchemaMapTransformer {
    void transform(
        String key, TsTableColumnSchema value, Map<String, TsTableColumnSchema> targetMap);
  }

  public int getTagColumnOrdinal(final String columnName) {
    readWriteLock.readLock().lock();
    try {
      return tagColumnIndexMap.getOrDefault(columnName.toLowerCase(), -1);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public List<TsTableColumnSchema> getTagColumnSchemaList() {
    Pair<Long, List<TsTableColumnSchema>> VersionAndTagColumnSchemas = tagColumnSchemas.get();
    if (VersionAndTagColumnSchemas != null
        && isNotWrite.get()
        && VersionAndTagColumnSchemas.getLeft() == instanceVersion.get()) {
      return VersionAndTagColumnSchemas.getRight();
    }

    readWriteLock.readLock().lock();
    try {
      List<TsTableColumnSchema> tagColumnSchemaList = new ArrayList<>(tagColumnIndexMap.size());
      for (final TsTableColumnSchema columnSchema : columnSchemaMap.values()) {
        if (TsTableColumnCategory.TAG.equals(columnSchema.getColumnCategory())) {
          tagColumnSchemaList.add(columnSchema);
        }
      }
      VersionAndTagColumnSchemas = new Pair<>(instanceVersion.get(), tagColumnSchemaList);
      return tagColumnSchemaList;
    } finally {
      tagColumnSchemas.set(VersionAndTagColumnSchemas);
      readWriteLock.readLock().unlock();
    }
  }

  // Currently only supports device view
  public void renameTable(final String newName) {
    executeWrite((colMap, tagMap, idMap) -> tableName = newName);
  }

  public void addColumnSchema(final TsTableColumnSchema columnSchema) {
    executeWrite(
        (colMap, tagMap, idMap) -> {
          colMap.put(columnSchema.getColumnName(), columnSchema);
          if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.TAG)) {
            tagNums++;
            tagMap.put(columnSchema.getColumnName(), tagNums - 1);
          } else if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.FIELD)) {
            fieldNum++;
          }
        });
  }

  public void renameColumnSchema(final String oldName, final String newName) {
    // Transform during copy: rename column while preserving insertion order in single pass
    executeWriteWithTransform(
        (key, schema, targetMap) -> {
          if (key.equals(oldName)) {
            // Rename this column while preserving its position
            final Map<String, String> oldProps = schema.getProps();
            oldProps.computeIfAbsent(TreeViewSchema.ORIGINAL_NAME, k -> schema.getColumnName());

            TsTableColumnSchema renamedSchema;
            switch (schema.getColumnCategory()) {
              case TAG:
                renamedSchema = new TagColumnSchema(newName, schema.getDataType(), oldProps);
                break;
              case FIELD:
                renamedSchema =
                    new FieldColumnSchema(
                        newName,
                        schema.getDataType(),
                        ((FieldColumnSchema) schema).getEncoding(),
                        ((FieldColumnSchema) schema).getCompressor(),
                        oldProps);
                break;
              case ATTRIBUTE:
                renamedSchema = new AttributeColumnSchema(newName, schema.getDataType(), oldProps);
                break;
              case TIME:
              default:
                // Do nothing for TIME column
                targetMap.put(key, schema);
                return;
            }
            targetMap.put(newName, renamedSchema);
          } else {
            targetMap.put(key, schema);
          }
        },
        (colMap, tagMap, idMap) -> {
          // No additional operation needed, transformation already done during copy
        });
  }

  public void removeColumnSchema(final String columnName) {
    executeWrite(
        (colMap, tagMap, idMap) -> {
          final TsTableColumnSchema columnSchema = colMap.get(columnName);
          if (columnSchema != null
              && columnSchema.getColumnCategory().equals(TsTableColumnCategory.TAG)) {
            throw new SchemaExecutionException("Cannot remove an tag column: " + columnName);
          } else if (columnSchema != null) {
            colMap.remove(columnName);
            if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.FIELD)) {
              fieldNum--;
            }
          }
        });
  }

  public int getColumnNum() {
    readWriteLock.readLock().lock();
    try {
      return columnSchemaMap.size();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public int getTagNum() {
    readWriteLock.readLock().lock();
    try {
      return tagNums;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public int getFieldNum() {
    readWriteLock.readLock().lock();
    try {
      return fieldNum;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public List<TsTableColumnSchema> getColumnList() {
    readWriteLock.readLock().lock();
    try {
      return new ArrayList<>(columnSchemaMap.values());
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  // This shall only be called on DataNode, where the tsTable is replaced completely thus an old
  // cache won't pollute the newest value
  public long getCachedTableTTL() {
    // Cache for performance
    if (ttlValue < 0) {
      ttlValue = getTableTTL();
    }
    return ttlValue;
  }

  public long getTableTTL() {
    final Optional<String> ttl = getPropValue(TTL_PROPERTY);
    return ttl.isPresent() && !ttl.get().equalsIgnoreCase(TTL_INFINITE)
        ? CommonDateTimeUtils.convertMilliTimeWithPrecision(
            Long.parseLong(ttl.get()),
            CommonDescriptor.getInstance().getConfig().getTimestampPrecision())
        : Long.MAX_VALUE;
  }

  public Map<String, String> getProps() {
    readWriteLock.readLock().lock();
    try {
      return props;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public Pair<Long, Long> getInstanceVersion() {
    return new Pair<>(creationId, instanceVersion.get());
  }

  public boolean containsPropWithoutLock(final String propKey) {
    return props != null && props.containsKey(propKey);
  }

  public Optional<String> getPropValue(final String propKey) {
    readWriteLock.readLock().lock();
    try {
      return props != null && props.containsKey(propKey)
          ? Optional.of(props.get(propKey))
          : Optional.empty();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void addProp(final String key, final String value) {
    executeWrite(
        (colMap, tagMap, idMap) -> {
          if (props == null) {
            props = new HashMap<>();
          }
          props.put(key, value);
        });
  }

  public void removeProp(final String key) {
    executeWrite(
        (colMap, tagMap, idMap) -> {
          if (props == null) {
            return;
          }
          props.remove(key);
        });
  }

  public void serialize(final OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(columnSchemaMap.size(), stream);
    for (final TsTableColumnSchema columnSchema : columnSchemaMap.values()) {
      TsTableColumnSchemaUtil.serialize(columnSchema, stream);
    }
    ReadWriteIOUtils.write(props, stream);
  }

  public static TsTable deserialize(final InputStream inputStream) throws IOException {
    final String name = ReadWriteIOUtils.readString(inputStream);
    final int columnNum = ReadWriteIOUtils.readInt(inputStream);
    if (columnNum < 0) {
      return new NonCommittableTsTable(name);
    }
    final TsTable table = new TsTable(name);
    for (int i = 0; i < columnNum; i++) {
      table.addColumnSchema(TsTableColumnSchemaUtil.deserialize(inputStream));
    }
    table.props = ReadWriteIOUtils.readMap(inputStream);
    return table;
  }

  public static TsTable deserialize(final ByteBuffer buffer) {
    final String name = ReadWriteIOUtils.readString(buffer);
    final int columnNum = ReadWriteIOUtils.readInt(buffer);
    if (columnNum < 0) {
      return new NonCommittableTsTable(name);
    }
    final TsTable table = new TsTable(name);
    for (int i = 0; i < columnNum; i++) {
      table.addColumnSchema(TsTableColumnSchemaUtil.deserialize(buffer));
    }
    table.props = ReadWriteIOUtils.readMap(buffer);
    return table;
  }

  public void setProps(Map<String, String> props) {
    executeWrite((colMap, tagMap, idMap) -> this.props = props);
  }

  public void checkTableNameAndObjectNames4Object() throws MetadataException {
    throw new MetadataException("The object type column is not supported.");
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName);
  }

  @Override
  public String toString() {
    return "TsTable{"
        + "tableName='"
        + tableName
        + '\''
        + ", columnSchemaMap="
        + columnSchemaMap
        + ", props="
        + props
        + '}';
  }
}
