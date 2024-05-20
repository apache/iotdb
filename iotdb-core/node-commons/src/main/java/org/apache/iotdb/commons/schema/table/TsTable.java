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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TsTable {

  private static final String TIME_COLUMN_NAME = "Time";
  private static final TimeColumnSchema TIME_COLUMN_SCHEMA =
      new TimeColumnSchema(TIME_COLUMN_NAME, TSDataType.INT64);

  private final String tableName;

  private final Map<String, TsTableColumnSchema> columnSchemaMap =
      Collections.synchronizedMap(new LinkedHashMap<>());

  private Map<String, String> props = null;

  private transient int idNums = 0;

  public TsTable(String tableName) {
    this.tableName = tableName;
    columnSchemaMap.put(TIME_COLUMN_NAME, TIME_COLUMN_SCHEMA);
  }

  public String getTableName() {
    return tableName;
  }

  public TsTableColumnSchema getColumnSchema(String columnName) {
    return columnSchemaMap.get(columnName);
  }

  public void addColumnSchema(TsTableColumnSchema columnSchema) {
    columnSchemaMap.put(columnSchema.getColumnName(), columnSchema);
    if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
      idNums++;
    }
  }

  public void removeColumnSchema(String columnName) {
    columnSchemaMap.remove(columnName);
  }

  public int getColumnNum() {
    return columnSchemaMap.size();
  }

  public int getIdNums() {
    return idNums;
  }

  public List<TsTableColumnSchema> getColumnList() {
    return new ArrayList<>(columnSchemaMap.values());
  }

  public String getPropValue(String propKey) {
    return props == null ? null : props.get(propKey);
  }

  public void addProp(String key, String value) {
    if (props == null) {
      synchronized (this) {
        if (props == null) {
          props = new ConcurrentHashMap<>();
        }
      }
    }
    props.put(key, value);
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

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName);
  }
}
