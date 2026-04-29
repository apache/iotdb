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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/** Serializer for PreparedStatement parameters. */
public class PreparedParameterSerde {

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

  private PreparedParameterSerde() {}

  /** Serialize parameters to binary format. */
  public static ByteBuffer serialize(Object[] values, int[] jdbcTypes, int count) {
    try {
      PublicBAOS outputStream = new PublicBAOS();
      ReadWriteIOUtils.write(count, outputStream);
      for (int i = 0; i < count; i++) {
        serializeParameter(outputStream, values[i], jdbcTypes[i]);
      }
      return ByteBuffer.wrap(outputStream.getBuf(), 0, outputStream.size());
    } catch (IOException e) {
      // Should not happen with PublicBAOS
      throw new IllegalStateException("Failed to serialize parameters", e);
    }
  }

  private static void serializeParameter(OutputStream outputStream, Object value, int jdbcType)
      throws IOException {
    if (value == null || jdbcType == Types.NULL) {
      ReadWriteIOUtils.write(TSDataType.UNKNOWN, outputStream);
      return;
    }

    switch (jdbcType) {
      case Types.BOOLEAN:
        ReadWriteIOUtils.write(TSDataType.BOOLEAN, outputStream);
        ReadWriteIOUtils.write((boolean) value, outputStream);
        break;

      case Types.INTEGER:
        ReadWriteIOUtils.write(TSDataType.INT32, outputStream);
        ReadWriteIOUtils.write(((Number) value).intValue(), outputStream);
        break;

      case Types.BIGINT:
        ReadWriteIOUtils.write(TSDataType.INT64, outputStream);
        ReadWriteIOUtils.write(((Number) value).longValue(), outputStream);
        break;

      case Types.FLOAT:
        ReadWriteIOUtils.write(TSDataType.FLOAT, outputStream);
        ReadWriteIOUtils.write(((Number) value).floatValue(), outputStream);
        break;

      case Types.DOUBLE:
        ReadWriteIOUtils.write(TSDataType.DOUBLE, outputStream);
        ReadWriteIOUtils.write(((Number) value).doubleValue(), outputStream);
        break;

      case Types.VARCHAR:
      case Types.CHAR:
        ReadWriteIOUtils.write(TSDataType.STRING, outputStream);
        ReadWriteIOUtils.write((String) value, outputStream);
        break;

      case Types.BINARY:
      case Types.VARBINARY:
        ReadWriteIOUtils.write(TSDataType.BLOB, outputStream);
        ReadWriteIOUtils.write(new Binary((byte[]) value), outputStream);
        break;

      default:
        ReadWriteIOUtils.write(TSDataType.STRING, outputStream);
        ReadWriteIOUtils.write(String.valueOf(value), outputStream);
        break;
    }
  }

  /** Deserialize parameters from binary format. */
  public static List<DeserializedParam> deserialize(ByteBuffer buffer) {
    if (buffer == null || buffer.remaining() == 0) {
      return new ArrayList<>();
    }

    buffer.rewind();
    int count = ReadWriteIOUtils.readInt(buffer);
    if (count < 0 || count > buffer.remaining()) {
      throw new IllegalArgumentException("Invalid parameter count: " + count);
    }

    List<DeserializedParam> result = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      TSDataType type = ReadWriteIOUtils.readDataType(buffer);
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
        return ReadWriteIOUtils.readBool(buffer);
      case INT32:
        return ReadWriteIOUtils.readInt(buffer);
      case INT64:
        return ReadWriteIOUtils.readLong(buffer);
      case FLOAT:
        return ReadWriteIOUtils.readFloat(buffer);
      case DOUBLE:
        return ReadWriteIOUtils.readDouble(buffer);
      case TEXT:
      case STRING:
        return ReadWriteIOUtils.readString(buffer);
      case BLOB:
        return ReadWriteIOUtils.readBinary(buffer).getValues();
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  /** Convert byte array to hexadecimal string representation. */
  public static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }
}
