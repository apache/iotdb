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
package org.apache.iotdb.tsfile.file.metadata.enums;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;

public enum TSDataType {
  /**
   * BOOLEAN
   */
  BOOLEAN(0),

  /**
   *
   */
  INT32(1),

  /**
   * INT64
   */
  INT64(2),

  /**
   * FLOAT
   */
  FLOAT(3),

  /**
   * DOUBLE
   */
  DOUBLE(4),

  /**
   * TEXT
   */
  TEXT(5);

  private final int type;

  TSDataType(int type) {
    this.type = type;
  }

  /**
   * give an integer to return a data type.
   *
   * @param type -param to judge enum type
   * @return -enum type
   */
  public static TSDataType deserialize(short type) {
    return getTsDataType(type);
  }


  private static TSDataType getTsDataType(short type) {
    for (TSDataType tsDataType : TSDataType.values()) {
      if (type == tsDataType.type) {
        return tsDataType;
      }
    }

    throw new IllegalArgumentException("Invalid input: " + type);
  }

  public static byte deserializeToByte(short type) {
    getTsDataType(type);
    return (byte) type;
  }

  /**
   * give an byte to return a data type.
   *
   * @param type byte number
   * @return data type
   */
  public static TSDataType byteToEnum(byte type) {
    return getTsDataType(type);
  }

  public static TSDataType deserializeFrom(ByteBuffer buffer) {
    return deserialize(buffer.getShort());
  }

  public static int getSerializedSize() {
    return Short.BYTES;
  }

  public void serializeTo(ByteBuffer byteBuffer) {
    byteBuffer.putShort(serialize());
  }

  public void serializeTo(DataOutputStream outputStream) throws IOException {
    outputStream.writeShort(serialize());
  }

  /**
   * return a serialize data type.
   *
   * @return -enum type
   */
  public short serialize() {
    return enumToByte();
  }

  public int getDataTypeSize() {
    switch (this) {
      case BOOLEAN:
        return 1;
      case INT32:
      case FLOAT:
        return 4;
        // For text: return the size of reference here
      case TEXT:
      case INT64:
      case DOUBLE:
        return 8;
      default:
        throw new UnSupportedDataTypeException(this.toString());
    }
  }

  /**
   * @return byte number
   */
  public byte enumToByte() {
    return (byte) type;
  }
}
