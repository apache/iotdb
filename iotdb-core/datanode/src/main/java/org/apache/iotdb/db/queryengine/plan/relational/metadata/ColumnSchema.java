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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;

import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.DateType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.read.common.type.UnknownType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;

public class ColumnSchema {
  private final String name;
  private final Type type;
  private final TsTableColumnCategory columnCategory;
  private final boolean hidden;

  public ColumnSchema(
      String name, Type type, boolean hidden, TsTableColumnCategory columnCategory) {
    requireNonNull(name, "name is null");

    this.name = name.toLowerCase(ENGLISH);
    this.type = type;
    this.columnCategory = columnCategory;
    this.hidden = hidden;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public TsTableColumnCategory getColumnCategory() {
    return columnCategory;
  }

  public boolean isHidden() {
    return hidden;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnSchema that = (ColumnSchema) o;
    return hidden == that.hidden && name.equals(that.name) && type.equals(that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, hidden);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ColumnSchema.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .add("type=" + type)
        .add("hidden=" + hidden)
        .toString();
  }

  public static void serialize(ColumnSchema columnSchema, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(columnSchema.getName(), byteBuffer);
    ReadWriteIOUtils.write(columnSchema.getType().getTypeEnum().ordinal(), byteBuffer);
    columnSchema.getColumnCategory().serialize(byteBuffer);
    ReadWriteIOUtils.write(columnSchema.isHidden(), byteBuffer);
  }

  public static void serialize(ColumnSchema columnSchema, DataOutputStream stream)
      throws IOException {
    ReadWriteIOUtils.write(columnSchema.getName(), stream);
    ReadWriteIOUtils.write(columnSchema.getType().getTypeEnum().ordinal(), stream);
    columnSchema.getColumnCategory().serialize(stream);
    ReadWriteIOUtils.write(columnSchema.isHidden(), stream);
  }

  public static ColumnSchema deserialize(ByteBuffer byteBuffer) {
    String name = ReadWriteIOUtils.readString(byteBuffer);
    TypeEnum typeEnum = TypeEnum.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    Type type = getType(typeEnum);
    TsTableColumnCategory columnCategory = TsTableColumnCategory.deserialize(byteBuffer);
    boolean isHidden = ReadWriteIOUtils.readBool(byteBuffer);

    return new ColumnSchema(name, type, isHidden, columnCategory);
  }

  public static Type getType(TypeEnum typeEnum) {
    switch (typeEnum) {
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case INT32:
        return INT32;
      case INT64:
        return INT64;
      case FLOAT:
        return FloatType.FLOAT;
      case DOUBLE:
        return DoubleType.DOUBLE;
      case TEXT:
        return BinaryType.TEXT;
      case STRING:
        return StringType.STRING;
      case BLOB:
        return BlobType.BLOB;
      case TIMESTAMP:
        return TimestampType.TIMESTAMP;
      case DATE:
        return DateType.DATE;
      default:
        return UnknownType.UNKNOWN;
    }
  }

  public static ColumnSchema ofTsColumnSchema(TsTableColumnSchema schema) {
    return new ColumnSchema(
        schema.getColumnName(),
        TypeFactory.getType(schema.getDataType()),
        false,
        schema.getColumnCategory());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(ColumnMetadata columnMetadata) {
    return new Builder(columnMetadata);
  }

  public static class Builder {
    private String name;
    private Type type;
    private TsTableColumnCategory columnCategory;
    private boolean hidden;

    private Builder() {}

    private Builder(ColumnMetadata columnMetadata) {
      this.name = columnMetadata.getName();
      this.type = columnMetadata.getType();
      this.hidden = columnMetadata.isHidden();
    }

    public Builder setName(String name) {
      this.name = requireNonNull(name, "name is null");
      return this;
    }

    public Builder setType(Type type) {
      this.type = requireNonNull(type, "type is null");
      return this;
    }

    public Builder setColumnCategory(TsTableColumnCategory columnCategory) {
      this.columnCategory = requireNonNull(columnCategory, "columnCategory is null");
      return this;
    }

    public Builder setHidden(boolean hidden) {
      this.hidden = hidden;
      return this;
    }

    public ColumnSchema build() {
      return new ColumnSchema(name, type, hidden, columnCategory);
    }
  }
}
