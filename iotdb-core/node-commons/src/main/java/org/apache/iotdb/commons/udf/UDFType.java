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

package org.apache.iotdb.commons.udf;

import org.apache.iotdb.common.rpc.thrift.FunctionType;
import org.apache.iotdb.common.rpc.thrift.Model;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** UDFType is an enum class that represents the type of UDF. */
public enum UDFType {
  TREE_AVAILABLE(Model.TREE, FunctionType.NONE, Available.AVAILABLE),
  TREE_UNAVAILABLE(Model.TREE, FunctionType.NONE, Available.UNAVAILABLE),

  TABLE_AVAILABLE_SCALAR(Model.TABLE, FunctionType.SCALAR, Available.AVAILABLE),
  TABLE_AVAILABLE_AGGREGATE(Model.TABLE, FunctionType.AGGREGATE, Available.AVAILABLE),
  TABLE_AVAILABLE_TABLE(Model.TABLE, FunctionType.TABLE, Available.AVAILABLE),
  TABLE_UNAVAILABLE_SCALAR(Model.TABLE, FunctionType.SCALAR, Available.UNAVAILABLE),
  TABLE_UNAVAILABLE_AGGREGATE(Model.TABLE, FunctionType.AGGREGATE, Available.UNAVAILABLE),
  TABLE_UNAVAILABLE_TABLE(Model.TABLE, FunctionType.TABLE, Available.UNAVAILABLE);

  private static final int MODEL_SHIFT = 7;
  private static final int TYPE_SHIFT = 2;
  private static final int AVAILABLE_SHIFT = 1;

  private static final byte MODEL_MASK = (byte) (0b10000000);
  private static final byte TYPE_MASK = (byte) 0b01111100;
  private static final byte AVAILABLE_MASK = (byte) 0b00000010;

  private final Model model;

  /** For Tree Model: none. For Table Model: scalar, aggregate, table. */
  private final FunctionType type;

  private final Available available;

  UDFType(Model model, FunctionType type, Available available) {
    this.model = model;
    this.type = type;
    this.available = available;
  }

  public static UDFType of(Model model, FunctionType type, boolean available) {
    if (model == Model.TREE) {
      return available ? TREE_AVAILABLE : TREE_UNAVAILABLE;
    } else {
      switch (type) {
        case SCALAR:
          return available ? TABLE_AVAILABLE_SCALAR : TABLE_UNAVAILABLE_SCALAR;
        case AGGREGATE:
          return available ? TABLE_AVAILABLE_AGGREGATE : TABLE_UNAVAILABLE_AGGREGATE;
        case TABLE:
          return available ? TABLE_AVAILABLE_TABLE : TABLE_UNAVAILABLE_TABLE;
        default:
          throw new IllegalArgumentException("Unknown FunctionType: " + type);
      }
    }
  }

  /**
   * ｜Model(0) | FunctionType(1-5)｜ Available(6) | placeholder(7) |
   *
   * <ul>
   *   <li>For tree model, type is always 0 for compatibility.
   *   <li>For table model, type is 1 for scalar, 2 for aggregate, 3 for table.
   *   <li>Available is 0 for available, 1 for unavailable.
   *   <li>Placeholder is always 0 for some unforeseen circumstances.
   * </ul>
   */
  public void serialize(DataOutputStream stream) throws IOException {
    byte value = 0;
    value |= (byte) (model.getValue() << MODEL_SHIFT);
    value |= (byte) (type.getValue() << TYPE_SHIFT);
    value |= (byte) (available.getValue() << AVAILABLE_SHIFT);
    stream.writeByte(value);
  }

  public static UDFType deserialize(ByteBuffer buffer) {
    byte readByte = ReadWriteIOUtils.readByte(buffer);
    for (UDFType udfType : values()) {
      if ((byte) (udfType.model.getValue() << MODEL_SHIFT) == (readByte & MODEL_MASK)
          && (byte) (udfType.type.getValue() << TYPE_SHIFT) == (readByte & TYPE_MASK)
          && (byte) (udfType.available.getValue() << AVAILABLE_SHIFT)
              == (readByte & AVAILABLE_MASK)) {
        return udfType;
      }
    }
    throw new IllegalArgumentException(
        "Unknown UDFType:"
            + String.format("%8s", Integer.toBinaryString(readByte & 0xFF)).replace(' ', '0'));
  }

  public boolean isTreeModel() {
    return this.model == Model.TREE;
  }

  public boolean isTableModel() {
    return this.model == Model.TABLE;
  }

  public boolean isAvailable() {
    return this.available == Available.AVAILABLE;
  }

  public FunctionType getType() {
    return type;
  }

  public UDFType setAvailable(boolean available) {
    if (this.isTreeModel()) {
      return available ? TREE_AVAILABLE : TREE_UNAVAILABLE;
    } else {
      switch (this.type) {
        case SCALAR:
          return available ? TABLE_AVAILABLE_SCALAR : TABLE_UNAVAILABLE_SCALAR;
        case AGGREGATE:
          return available ? TABLE_AVAILABLE_AGGREGATE : TABLE_UNAVAILABLE_AGGREGATE;
        case TABLE:
          return available ? TABLE_AVAILABLE_TABLE : TABLE_UNAVAILABLE_TABLE;
        default:
          throw new IllegalArgumentException("Unknown FunctionType: " + type);
      }
    }
  }

  private enum Available {
    AVAILABLE((byte) 0),
    UNAVAILABLE((byte) 1);

    private final byte value;

    Available(byte value) {
      this.value = value;
    }

    public byte getValue() {
      return value;
    }
  }
}
