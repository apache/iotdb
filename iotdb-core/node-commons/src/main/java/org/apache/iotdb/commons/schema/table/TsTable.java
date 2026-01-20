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
  public static final String TTL_PROPERTY = "ttl";
  public static final Set<String> TABLE_ALLOWED_PROPERTIES = Collections.singleton(TTL_PROPERTY);
  private static final String OBJECT_STRING_ERROR =
      "When there are object fields, the %s %s shall not be '.', '..' or contain './', '.\\'.";
  protected String tableName;

  private Map<String, TsTableColumnSchema> columnSchemaMap = new LinkedHashMap<>();
  private final Map<String, Integer> tagColumnIndexMap = new HashMap<>();
  private final Map<String, Integer> idColumnIndexMap = new HashMap<>();

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
   * Execute a write operation with optimistic lock support. This method handles the write flag and
   * version increment automatically.
   *
   * @param writeOperation the write operation to execute
   */
  private void executeWrite(Runnable writeOperation) {
    readWriteLock.writeLock().lock();
    isNotWrite.set(false);
    try {
      writeOperation.run();
    } finally {
      instanceVersion.incrementAndGet();
      isNotWrite.set(true);
      readWriteLock.writeLock().unlock();
    }
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
    executeWrite(() -> tableName = newName);
  }

  public void addColumnSchema(final TsTableColumnSchema columnSchema) {
    executeWrite(
        () -> {
          columnSchemaMap.put(columnSchema.getColumnName(), columnSchema);
          if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.TAG)) {
            tagNums++;
            tagColumnIndexMap.put(columnSchema.getColumnName(), tagNums - 1);
          } else if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.FIELD)) {
            fieldNum++;
          }
        });
  }

  /**
   * Renames a column in the table schema while strictly preserving its original ordinal position.
   */
  public void renameColumnSchema(final String oldName, final String newName) {
    executeWrite(
        () -> {
          if (!columnSchemaMap.containsKey(oldName)) {
            return;
          }

          // Capture the current strict order of current columns
          List<Map.Entry<String, TsTableColumnSchema>> snapshotOfColumns =
              new ArrayList<>(columnSchemaMap.entrySet());
          columnSchemaMap.clear();

          // Re-insert all entries in their original sequence, substituting the renamed column at
          // its exact original index
          for (Map.Entry<String, TsTableColumnSchema> entry : snapshotOfColumns) {
            String currentKey = entry.getKey();
            TsTableColumnSchema currentColumnSchema = entry.getValue();

            if (currentKey.equals(oldName)) {
              TsTableColumnSchema newSchema = createRenamedSchema(currentColumnSchema, newName);
              columnSchemaMap.put(newName, newSchema);
            } else {
              columnSchemaMap.put(currentKey, currentColumnSchema);
            }
          }
        });
  }

  private TsTableColumnSchema createRenamedSchema(TsTableColumnSchema oldSchema, String newName) {
    Map<String, String> oldProps = oldSchema.getProps();
    oldProps.computeIfAbsent(TreeViewSchema.ORIGINAL_NAME, k -> oldSchema.getColumnName());

    switch (oldSchema.getColumnCategory()) {
      case TAG:
        return new TagColumnSchema(newName, oldSchema.getDataType(), oldProps);
      case FIELD:
        return new FieldColumnSchema(
            newName,
            oldSchema.getDataType(),
            ((FieldColumnSchema) oldSchema).getEncoding(),
            ((FieldColumnSchema) oldSchema).getCompressor(),
            oldProps);
      case ATTRIBUTE:
        return new AttributeColumnSchema(newName, oldSchema.getDataType(), oldProps);
      case TIME:
        return new TimeColumnSchema(newName, oldSchema.getDataType(), oldProps);
      default:
        return oldSchema;
    }
  }

  public void removeColumnSchema(final String columnName) {
    executeWrite(
        () -> {
          final TsTableColumnSchema columnSchema = columnSchemaMap.get(columnName);
          if (columnSchema != null
              && columnSchema.getColumnCategory().equals(TsTableColumnCategory.TAG)) {
            throw new SchemaExecutionException("Cannot remove an tag column: " + columnName);
          } else if (columnSchema != null) {
            columnSchemaMap.remove(columnName);
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
        () -> {
          if (props == null) {
            props = new HashMap<>();
          }
          props.put(key, value);
        });
  }

  public void removeProp(final String key) {
    executeWrite(
        () -> {
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
    executeWrite(() -> this.props = props);
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
