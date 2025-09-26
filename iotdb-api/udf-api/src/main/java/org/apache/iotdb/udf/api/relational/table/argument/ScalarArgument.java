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

import org.apache.tsfile.utils.Binary;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;

public class ScalarArgument implements Argument {
  private final Type type;
  private final Object value;

  public ScalarArgument(Type type, Object value) {
    this.type = type;
    this.value = value;
  }

  public Type getType() {
    return type;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(ArgumentType.SCALAR_ARGUMENT.ordinal());
    buffer.put(type.getType());
    switch (type) {
      case BOOLEAN:
        buffer.put((boolean) value ? (byte) 1 : (byte) 0);
        break;
      case INT32:
        buffer.putInt((int) value);
        break;
      case INT64:
      case TIMESTAMP:
        buffer.putLong((long) value);
        break;
      case FLOAT:
        buffer.putFloat((float) value);
        break;
      case DOUBLE:
        buffer.putDouble((double) value);
        break;
      case DATE:
        buffer.putLong(((LocalDate) value).toEpochDay());
        break;
      case TEXT:
      case STRING:
        byte[] bytes = ((String) value).getBytes();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        break;
      case BLOB:
        byte[] blobBytes = ((Binary) value).getValues();
        buffer.putInt(blobBytes.length);
        buffer.put(blobBytes);
        break;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  @Override
  public void serialize(DataOutputStream buffer) throws IOException {
    buffer.writeInt(ArgumentType.SCALAR_ARGUMENT.ordinal());
    buffer.writeByte(type.getType());
    switch (type) {
      case BOOLEAN:
        buffer.writeByte((byte) value);
        break;
      case INT32:
        buffer.writeInt((int) value);
        break;
      case INT64:
      case TIMESTAMP:
        buffer.writeLong((long) value);
        break;
      case FLOAT:
        buffer.writeFloat((float) value);
        break;
      case DOUBLE:
        buffer.writeDouble((double) value);
        break;
      case DATE:
        buffer.writeLong(((LocalDate) value).toEpochDay());
        break;
      case TEXT:
      case STRING:
        byte[] bytes = ((String) value).getBytes();
        buffer.writeInt(bytes.length);
        buffer.write(bytes);
        break;
      case BLOB:
        byte[] blobBytes = ((Binary) value).getValues();
        buffer.writeInt(blobBytes.length);
        buffer.write(blobBytes);
        break;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  public static ScalarArgument deserialize(ByteBuffer buffer) {
    Type type = Type.valueOf(buffer.get());
    switch (type) {
      case BOOLEAN:
        return new ScalarArgument(type, buffer.get() != 0);
      case INT32:
        return new ScalarArgument(type, buffer.getInt());
      case INT64:
      case TIMESTAMP:
        return new ScalarArgument(type, buffer.getLong());
      case FLOAT:
        return new ScalarArgument(type, buffer.getFloat());
      case DOUBLE:
        return new ScalarArgument(type, buffer.getDouble());
      case DATE:
        return new ScalarArgument(type, LocalDate.ofEpochDay(buffer.getLong()));
      case BLOB:
        byte[] blobBytes = new byte[buffer.getInt()];
        buffer.get(blobBytes);
        return new ScalarArgument(type, new Binary(blobBytes));
      case TEXT:
      case STRING:
        byte[] bytes = new byte[buffer.getInt()];
        buffer.get(bytes);
        return new ScalarArgument(type, new String(bytes));
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }
}
