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

package org.apache.iotdb.rpc.stmt;

import org.apache.tsfile.enums.TSDataType;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for PreparedStatement parameters.
 *
 * <p>Binary format: [paramCount:4bytes][param1][param2]...
 *
 * <p>Each parameter: [type:1byte][value:variable]
 */
public class PreparedParameterSerializer {

  public static class DeserializedParam {
    public final TSDataType type;
    public final Object value;

    DeserializedParam(TSDataType type, Object value) {
      this.type = type;
      this.value = value;
    }

    public boolean isNull() {
      return type == TSDataType.UNKNOWN || value == null;
    }
  }

  private PreparedParameterSerializer() {}

  // ================== Serialize (Client Side) ==================

  /**
   * Serialize parameters to binary format.
   *
   * @param values parameter values
   * @param jdbcTypes JDBC type codes (java.sql.Types)
   * @param count number of parameters
   * @return ByteBuffer containing serialized parameters
   */
  public static ByteBuffer serialize(Object[] values, int[] jdbcTypes, int count) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);

      dos.writeInt(count);
      for (int i = 0; i < count; i++) {
        serializeParameter(dos, values[i], jdbcTypes[i]);
      }

      dos.flush();
      return ByteBuffer.wrap(baos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize parameters", e);
    }
  }

  private static void serializeParameter(DataOutputStream dos, Object value, int jdbcType)
      throws IOException {
    if (value == null || jdbcType == Types.NULL) {
      dos.writeByte(TSDataType.UNKNOWN.serialize());
      return;
    }

    switch (jdbcType) {
      case Types.BOOLEAN:
        dos.writeByte(TSDataType.BOOLEAN.serialize());
        dos.writeByte((Boolean) value ? 1 : 0);
        break;

      case Types.INTEGER:
        dos.writeByte(TSDataType.INT32.serialize());
        dos.writeInt(((Number) value).intValue());
        break;

      case Types.BIGINT:
        dos.writeByte(TSDataType.INT64.serialize());
        dos.writeLong(((Number) value).longValue());
        break;

      case Types.FLOAT:
        dos.writeByte(TSDataType.FLOAT.serialize());
        dos.writeFloat(((Number) value).floatValue());
        break;

      case Types.DOUBLE:
        dos.writeByte(TSDataType.DOUBLE.serialize());
        dos.writeDouble(((Number) value).doubleValue());
        break;

      case Types.VARCHAR:
      case Types.CHAR:
        byte[] strBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
        dos.writeByte(TSDataType.STRING.serialize());
        dos.writeInt(strBytes.length);
        dos.write(strBytes);
        break;

      case Types.BINARY:
      case Types.VARBINARY:
        byte[] binBytes = (byte[]) value;
        dos.writeByte(TSDataType.BLOB.serialize());
        dos.writeInt(binBytes.length);
        dos.write(binBytes);
        break;

      default:
        byte[] defaultBytes = String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        dos.writeByte(TSDataType.STRING.serialize());
        dos.writeInt(defaultBytes.length);
        dos.write(defaultBytes);
        break;
    }
  }

  // ================== Deserialize (Server Side) ==================

  /**
   * Deserialize parameters from binary format.
   *
   * @param buffer ByteBuffer containing serialized parameters
   * @return list of deserialized parameters with type and value
   */
  public static List<DeserializedParam> deserialize(ByteBuffer buffer) {
    if (buffer == null || buffer.remaining() == 0) {
      return new ArrayList<>();
    }

    buffer.rewind();
    int count = buffer.getInt();
    if (count < 0 || count > buffer.remaining()) {
      throw new IllegalArgumentException("Invalid parameter count: " + count);
    }

    List<DeserializedParam> result = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      byte typeCode = buffer.get();
      TSDataType type = TSDataType.deserialize(typeCode);
      Object value = deserializeValue(buffer, type);
      result.add(new DeserializedParam(type, value));
    }

    return result;
  }

  private static Object deserializeValue(ByteBuffer buffer, TSDataType type) {
    switch (type) {
      case UNKNOWN:
        return null;
      case BOOLEAN:
        return buffer.get() != 0;
      case INT32:
        return buffer.getInt();
      case INT64:
        return buffer.getLong();
      case FLOAT:
        return buffer.getFloat();
      case DOUBLE:
        return buffer.getDouble();
      case TEXT:
      case STRING:
        int strLen = buffer.getInt();
        byte[] strBytes = new byte[strLen];
        buffer.get(strBytes);
        return new String(strBytes, StandardCharsets.UTF_8);
      case BLOB:
        int binLen = buffer.getInt();
        byte[] binBytes = new byte[binLen];
        buffer.get(binBytes);
        return binBytes;
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }
}
