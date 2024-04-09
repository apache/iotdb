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

import org.apache.iotdb.commons.schema.table.column.ColumnSchema;
import org.apache.iotdb.commons.schema.table.column.ColumnSchemaUtil;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TsTable {

  private final String tableName;

  private final Map<String, ColumnSchema> columnSchemaMap =
      Collections.synchronizedMap(new LinkedHashMap<>());

  private Map<String, String> props = null;

  public TsTable(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public ColumnSchema getColumnSchema(String columnName) {
    return columnSchemaMap.get(columnName);
  }

  public void addColumnSchema(ColumnSchema columnSchema) {
    columnSchemaMap.put(columnSchema.getColumnName(), columnSchema);
  }

  public List<ColumnSchema> getColumnList() {
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
    for (ColumnSchema columnSchema : columnSchemaMap.values()) {
      ColumnSchemaUtil.serialize(columnSchema, stream);
    }
    ReadWriteIOUtils.write(props, stream);
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
