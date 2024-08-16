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

import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.concurrent.ThreadSafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class TsTable {

  public static final String TIME_COLUMN_NAME = "time";
  private static final TimeColumnSchema TIME_COLUMN_SCHEMA =
      new TimeColumnSchema(TIME_COLUMN_NAME, TSDataType.TIMESTAMP);

  public static final Map<String, Object> TABLE_ALLOWED_PROPERTIES_2_DEFAULT_VALUE_MAP =
      new HashMap<>();

  public static final String TTL_PROPERTY = "TTL";

  static {
    TABLE_ALLOWED_PROPERTIES_2_DEFAULT_VALUE_MAP.put(
        TTL_PROPERTY.toLowerCase(Locale.ENGLISH), new Binary("INF", TSFileConfig.STRING_CHARSET));
  }

  private final String tableName;

  private final Map<String, TsTableColumnSchema> columnSchemaMap = new LinkedHashMap<>();

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private Map<String, String> props = null;

  private transient int idNums = 0;

  public TsTable(final String tableName) {
    this.tableName = tableName;
    columnSchemaMap.put(TIME_COLUMN_NAME, TIME_COLUMN_SCHEMA);
  }

  public String getTableName() {
    return tableName;
  }

  public TsTableColumnSchema getColumnSchema(final String columnName) {
    readWriteLock.readLock().lock();
    try {
      return columnSchemaMap.get(columnName);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void addColumnSchema(final TsTableColumnSchema columnSchema) {
    readWriteLock.writeLock().lock();
    try {
      columnSchemaMap.put(columnSchema.getColumnName(), columnSchema);
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
        idNums++;
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void removeColumnSchema(String columnName) {
    readWriteLock.writeLock().lock();
    try {
      TsTableColumnSchema columnSchema = columnSchemaMap.remove(columnName);
      if (columnSchema != null
          && columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
        idNums--;
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public int getColumnNum() {
    readWriteLock.readLock().lock();
    try {
      return columnSchemaMap.size();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public int getIdNums() {
    readWriteLock.readLock().lock();
    try {
      return idNums;
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
    readWriteLock.writeLock().lock();
    try {
      if (props == null) {
        props = new HashMap<>();
      }
      props.put(key, value);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void removeProp(final String key) {
    readWriteLock.writeLock().lock();
    try {
      if (props == null) {
        return;
      }
      props.remove(key);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public byte[] serialize() {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      serialize(stream);
    } catch (IOException ignored) {
      // won't happen
    }
    return stream.toByteArray();
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(columnSchemaMap.size(), stream);
    for (TsTableColumnSchema columnSchema : columnSchemaMap.values()) {
      TsTableColumnSchemaUtil.serialize(columnSchema, stream);
    }
    ReadWriteIOUtils.write(props, stream);
  }

  public static TsTable deserialize(InputStream inputStream) throws IOException {
    String name = ReadWriteIOUtils.readString(inputStream);
    TsTable table = new TsTable(name);
    int columnNum = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < columnNum; i++) {
      table.addColumnSchema(TsTableColumnSchemaUtil.deserialize(inputStream));
    }
    table.props = ReadWriteIOUtils.readMap(inputStream);
    return table;
  }

  public static TsTable deserialize(ByteBuffer buffer) {
    String name = ReadWriteIOUtils.readString(buffer);
    TsTable table = new TsTable(name);
    int columnNum = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < columnNum; i++) {
      table.addColumnSchema(TsTableColumnSchemaUtil.deserialize(buffer));
    }
    table.props = ReadWriteIOUtils.readMap(buffer);
    return table;
  }

  public void setProps(Map<String, String> props) {
    readWriteLock.writeLock().lock();
    try {
      this.props = props;
    } finally {
      readWriteLock.writeLock().unlock();
    }
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
