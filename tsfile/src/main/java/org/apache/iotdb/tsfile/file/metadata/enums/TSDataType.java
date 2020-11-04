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
  BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT;

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
    if (type >= 6 || type < 0) {
      throw new IllegalArgumentException("Invalid input: " + type);
    }
    switch (type) {
      case 0:
        return BOOLEAN;
      case 1:
        return INT32;
      case 2:
        return INT64;
      case 3:
        return FLOAT;
      case 4:
        return DOUBLE;
      default:
        return TEXT;
    }
  }

  public static byte deserializeToByte(short type) {
    if (type >= 6 || type < 0) {
      throw new IllegalArgumentException("Invalid input: " + type);
    }
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
    switch (this) {
      case BOOLEAN:
        return 0;
      case INT32:
        return 1;
      case INT64:
        return 2;
      case FLOAT:
        return 3;
      case DOUBLE:
        return 4;
      case TEXT:
        return 5;
      default:
        return -1;
    }
  }
}
