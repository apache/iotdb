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

package org.apache.iotdb.commons.schema.column;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ColumnHeader {

  private final String columnName;
  private final TSDataType dataType;
  private final String alias;

  public ColumnHeader(String columnName, TSDataType dataType, String alias) {
    this.columnName = columnName;
    this.dataType = dataType;
    this.alias = alias;
  }

  public ColumnHeader(String columnName, TSDataType dataType) {
    this.columnName = columnName;
    this.dataType = dataType;
    this.alias = null;
  }

  public String getColumnNameWithAlias() {
    if (alias != null) {
      return alias;
    }
    return columnName;
  }

  public String getColumnName() {
    return columnName;
  }

  public TSDataType getColumnType() {
    return dataType;
  }

  public boolean hasAlias() {
    return alias != null && !alias.isEmpty();
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(columnName, byteBuffer);
    ReadWriteIOUtils.write(dataType.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(hasAlias(), byteBuffer);
    if (hasAlias()) {
      ReadWriteIOUtils.write(alias, byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(columnName, stream);
    ReadWriteIOUtils.write(dataType.ordinal(), stream);
    ReadWriteIOUtils.write(hasAlias(), stream);
    if (hasAlias()) {
      ReadWriteIOUtils.write(alias, stream);
    }
  }

  public static ColumnHeader deserialize(ByteBuffer byteBuffer) {
    String columnName = ReadWriteIOUtils.readString(byteBuffer);
    TSDataType dataType = TSDataType.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    String alias = null;
    boolean hasAlias = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasAlias) {
      alias = ReadWriteIOUtils.readString(byteBuffer);
    }
    return new ColumnHeader(columnName, dataType, alias);
  }

  @Override
  public String toString() {
    if (hasAlias()) {
      return String.format("%s(%s) [%s]", columnName, alias, dataType);
    }
    return String.format("%s [%s]", columnName, dataType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnHeader that = (ColumnHeader) o;
    return Objects.equals(columnName, that.columnName)
        && dataType == that.dataType
        && Objects.equals(alias, that.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, dataType, alias);
  }
}
