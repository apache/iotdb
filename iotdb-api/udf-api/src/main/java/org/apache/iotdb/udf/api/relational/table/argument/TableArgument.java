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

package org.apache.iotdb.udf.api.relational.table.argument;

import org.apache.iotdb.udf.api.type.Type;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableArgument implements Argument {
  private final List<Optional<String>> fieldNames;
  private final List<Type> fieldTypes;
  private final List<String> partitionBy;
  private final List<String> orderBy;
  private final boolean rowSemantics;

  public TableArgument(
      List<Optional<String>> fieldNames,
      List<Type> fieldTypes,
      List<String> partitionBy,
      List<String> orderBy,
      boolean rowSemantic) {
    this.fieldNames = requireNonNull(fieldNames, "fieldNames is null");
    this.fieldTypes = requireNonNull(fieldTypes, "fieldTypes is null");
    if (fieldNames.size() != fieldTypes.size()) {
      throw new IllegalArgumentException("fieldNames and fieldTypes must have the same size");
    }
    this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
    this.orderBy = requireNonNull(orderBy, "orderBy is null");
    this.rowSemantics = rowSemantic;
  }

  public List<Optional<String>> getFieldNames() {
    return fieldNames;
  }

  public List<Type> getFieldTypes() {
    return fieldTypes;
  }

  public List<String> getPartitionBy() {
    return partitionBy;
  }

  public List<String> getOrderBy() {
    return orderBy;
  }

  public boolean isRowSemantics() {
    return rowSemantics;
  }

  public int size() {
    return fieldTypes.size();
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(ArgumentType.TABLE_ARGUMENT.ordinal());
    buffer.putInt(fieldNames.size());
    for (Optional<String> fieldName : fieldNames) {
      if (fieldName.isPresent()) {
        buffer.put((byte) 1);
        byte[] bytes = fieldName.get().getBytes();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
      } else {
        buffer.put((byte) 0);
      }
    }
    for (Type fieldType : fieldTypes) {
      buffer.put(fieldType.getType());
    }
    buffer.putInt(partitionBy.size());
    for (String partition : partitionBy) {
      byte[] bytes = partition.getBytes();
      buffer.putInt(bytes.length);
      buffer.put(bytes);
    }
    buffer.putInt(orderBy.size());
    for (String order : orderBy) {
      byte[] bytes = order.getBytes();
      buffer.putInt(bytes.length);
      buffer.put(bytes);
    }
    buffer.put((byte) (rowSemantics ? 1 : 0));
  }

  @Override
  public void serialize(DataOutputStream buffer) throws IOException {
    buffer.writeInt(ArgumentType.TABLE_ARGUMENT.ordinal());
    buffer.writeInt(fieldNames.size());
    for (Optional<String> fieldName : fieldNames) {
      if (fieldName.isPresent()) {
        buffer.writeByte((byte) 1);
        byte[] bytes = fieldName.get().getBytes();
        buffer.writeInt(bytes.length);
        buffer.write(bytes);
      } else {
        buffer.writeByte((byte) 0);
      }
    }
    for (Type fieldType : fieldTypes) {
      buffer.writeByte(fieldType.getType());
    }
    buffer.writeInt(partitionBy.size());
    for (String partition : partitionBy) {
      byte[] bytes = partition.getBytes();
      buffer.writeInt(bytes.length);
      buffer.write(bytes);
    }
    buffer.writeInt(orderBy.size());
    for (String order : orderBy) {
      byte[] bytes = order.getBytes();
      buffer.writeInt(bytes.length);
      buffer.write(bytes);
    }
    buffer.writeByte((byte) (rowSemantics ? 1 : 0));
  }

  public static TableArgument deserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    List<Optional<String>> fieldNames = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      if (buffer.get() == 1) {
        byte[] bytes = new byte[buffer.getInt()];
        buffer.get(bytes);
        fieldNames.add(Optional.of(new String(bytes)));
      } else {
        fieldNames.add(Optional.empty());
      }
    }
    List<Type> fieldTypes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      fieldTypes.add(Type.valueOf(buffer.get()));
    }
    size = buffer.getInt();
    List<String> partitionBy = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      byte[] bytes = new byte[buffer.getInt()];
      buffer.get(bytes);
      partitionBy.add(new String(bytes));
    }
    size = buffer.getInt();
    List<String> orderBy = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      byte[] bytes = new byte[buffer.getInt()];
      buffer.get(bytes);
      orderBy.add(new String(bytes));
    }
    boolean rowSemantics = buffer.get() == 1;
    return new TableArgument(fieldNames, fieldTypes, partitionBy, orderBy, rowSemantics);
  }
}
