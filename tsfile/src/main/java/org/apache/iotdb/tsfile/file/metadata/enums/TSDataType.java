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

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum TSDataType {
  /** BOOLEAN */
  BOOLEAN((byte) 0),

  /** INT32 */
  INT32((byte) 1),

  /** INT64 */
  INT64((byte) 2),

  /** FLOAT */
  FLOAT((byte) 3),

  /** DOUBLE */
  DOUBLE((byte) 4),

  /** TEXT */
  TEXT((byte) 5),

  /** VECTOR */
  VECTOR((byte) 6);

  private final byte type;

  TSDataType(byte type) {
    this.type = type;
  }

  /**
   * give an integer to return a data type.
   *
   * @param type -param to judge enum type
   * @return -enum type
   */
  public static TSDataType deserialize(byte type) {
    return getTsDataType(type);
  }

  private static TSDataType getTsDataType(byte type) {
    switch (type) {
      case 0:
        return TSDataType.BOOLEAN;
      case 1:
        return TSDataType.INT32;
      case 2:
        return TSDataType.INT64;
      case 3:
        return TSDataType.FLOAT;
      case 4:
        return TSDataType.DOUBLE;
      case 5:
        return TSDataType.TEXT;
      case 6:
        return TSDataType.VECTOR;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }

  public static TSDataType deserializeFrom(ByteBuffer buffer) {
    return deserialize(buffer.get());
  }

  public static int getSerializedSize() {
    return Byte.BYTES;
  }

  public void serializeTo(ByteBuffer byteBuffer) {
    byteBuffer.put(serialize());
  }

  public void serializeTo(DataOutputStream outputStream) throws IOException {
    outputStream.write(serialize());
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
      case VECTOR:
        return 8;
      default:
        throw new UnSupportedDataTypeException(this.toString());
    }
  }

  /** @return byte number */
  public byte serialize() {
    return type;
  }
}
