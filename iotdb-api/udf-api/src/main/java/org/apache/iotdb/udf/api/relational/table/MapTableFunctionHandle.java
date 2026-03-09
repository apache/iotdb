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

package org.apache.iotdb.udf.api.relational.table;

import org.apache.iotdb.udf.api.type.Type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MapTableFunctionHandle implements TableFunctionHandle {
  private static final Set<Class<?>> SUPPORT_VALUE_TYPE =
      new HashSet<>(
          Arrays.asList(
              Integer.class, Long.class, Double.class, Float.class, String.class, Boolean.class));
  private final Map<String, Object> map = new HashMap<>();

  public void addProperty(String key, Object value) {
    if (!SUPPORT_VALUE_TYPE.contains(value.getClass())) {
      throw new IllegalArgumentException("Unsupported value type.");
    }
    map.put(key, value);
  }

  public Object getProperty(String key) {
    return map.get(key);
  }

  @Override
  public byte[] serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(calculateSerializeSize());
    buffer.putInt(map.size());
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      byte[] bytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
      buffer.putInt(bytes.length);
      buffer.put(bytes);
      if (entry.getValue() instanceof Long) {
        buffer.put(Type.INT64.getType());
        buffer.putLong((Long) entry.getValue());
      } else if (entry.getValue() instanceof Integer) {
        buffer.put(Type.INT32.getType());
        buffer.putInt((Integer) entry.getValue());
      } else if (entry.getValue() instanceof Double) {
        buffer.put(Type.DOUBLE.getType());
        buffer.putDouble((Double) entry.getValue());
      } else if (entry.getValue() instanceof Float) {
        buffer.put(Type.FLOAT.getType());
        buffer.putFloat((Float) entry.getValue());
      } else if (entry.getValue() instanceof Boolean) {
        buffer.put(Type.BOOLEAN.getType());
        buffer.put(Boolean.TRUE.equals(entry.getValue()) ? (byte) 1 : (byte) 0);
      } else if (entry.getValue() instanceof String) {
        buffer.put(Type.STRING.getType());
        bytes = ((String) entry.getValue()).getBytes(StandardCharsets.UTF_8);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
      }
    }
    return buffer.array();
  }

  private int calculateSerializeSize() {
    int size = Integer.SIZE;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      size += Integer.BYTES + entry.getKey().getBytes(StandardCharsets.UTF_8).length + Byte.BYTES;
      if (entry.getValue() instanceof Long) {
        size += Long.BYTES;
      } else if (entry.getValue() instanceof Integer) {
        size += Integer.BYTES;
      } else if (entry.getValue() instanceof Double) {
        size += Double.BYTES;
      } else if (entry.getValue() instanceof Float) {
        size += Float.BYTES;
      } else if (entry.getValue() instanceof Boolean) {
        size += Byte.BYTES;
      } else if (entry.getValue() instanceof String) {
        byte[] bytes = ((String) entry.getValue()).getBytes(StandardCharsets.UTF_8);
        size += Integer.BYTES + bytes.length;
      }
    }
    return size;
  }

  @Override
  public void deserialize(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      byte[] b = new byte[buffer.getInt()];
      buffer.get(b);
      String key = new String(b, StandardCharsets.UTF_8);
      Type type = Type.valueOf(buffer.get());
      switch (type) {
        case BOOLEAN:
          map.put(key, buffer.get() != 0);
          break;
        case INT32:
          map.put(key, buffer.getInt());
          break;
        case INT64:
          map.put(key, buffer.getLong());
          break;
        case FLOAT:
          map.put(key, buffer.getFloat());
          break;
        case DOUBLE:
          map.put(key, buffer.getDouble());
          break;
        case STRING:
          b = new byte[buffer.getInt()];
          buffer.get(b);
          map.put(key, new String(b, StandardCharsets.UTF_8));
          break;
        default:
          throw new IllegalArgumentException("Unknown type: " + type);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MapTableFunctionHandle{");
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      sb.append(entry.getKey()).append("=").append(entry.getValue()).append(", ");
    }
    if (sb.length() > 2) {
      sb.setLength(sb.length() - 2); // remove last comma and space
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MapTableFunctionHandle handle = (MapTableFunctionHandle) o;
    return Objects.equals(map, handle.map);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(map);
  }

  public static class Builder {
    private final Map<String, Object> map = new HashMap<>();

    public Builder addProperty(String key, Object value) {
      if (!SUPPORT_VALUE_TYPE.contains(value.getClass())) {
        throw new IllegalArgumentException("Unsupported value type.");
      }
      map.put(key, value);
      return this;
    }

    public MapTableFunctionHandle build() {
      MapTableFunctionHandle handle = new MapTableFunctionHandle();
      handle.map.putAll(map);
      return handle;
    }
  }
}
