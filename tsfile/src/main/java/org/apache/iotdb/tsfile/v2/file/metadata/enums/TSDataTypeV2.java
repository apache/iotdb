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
package org.apache.iotdb.tsfile.v2.file.metadata.enums;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class TSDataTypeV2 {

  private TSDataTypeV2() {
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
    if (type >= 6 || type < 0) {
      throw new IllegalArgumentException("Invalid input: " + type);
    }
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
      default:
        return TSDataType.TEXT;
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
}
