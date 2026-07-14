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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.TsPrimitiveType;

import static org.apache.tsfile.enums.TSDataType.BLOB;
import static org.apache.tsfile.enums.TSDataType.STRING;
import static org.apache.tsfile.enums.TSDataType.TEXT;

public class Utils {
  public static final String UNSUPPORTED_TYPE_MESSAGE = "Unsupported data type : %s";

  private Utils() {}

  public static void serializeValue(
      TSDataType dataType, TsPrimitiveType value, byte[] valueBytes, int offset) {
    Type.fromTsDataType(dataType).toBytes(value, valueBytes, offset);
  }

  public static void serializeBinaryValue(Binary binary, byte[] valueBytes, int offset) {
    BytesUtils.intToBytes(binary.getValues().length, valueBytes, offset);
    offset += Integer.BYTES;
    System.arraycopy(binary.getValues(), 0, valueBytes, offset, binary.getValues().length);
  }

  public static byte[] serializeTimeValueWithNull(
      TSDataType seriesDataType, long time, boolean isOrderTimeNull, TsPrimitiveType value) {

    byte[] valueBytes = new byte[9 + calcTypeSize(seriesDataType, value)];
    BytesUtils.longToBytes(time, valueBytes, 0);
    BytesUtils.boolToBytes(isOrderTimeNull, valueBytes, 8);
    serializeValue(seriesDataType, value, valueBytes, 9);
    return valueBytes;
  }

  public static byte[] serializeTimeValueWithNull(
      TSDataType seriesDataType,
      long time,
      boolean valueIsNull,
      boolean isOrderTimeNull,
      TsPrimitiveType value) {
    // Allocate buffer: fixed header size (10 bytes) + dynamic value size if present
    byte[] valueBytes =
        valueIsNull ? new byte[10] : new byte[10 + calcTypeSize(seriesDataType, value)];
    BytesUtils.longToBytes(time, valueBytes, 0);
    BytesUtils.boolToBytes(isOrderTimeNull, valueBytes, 8);
    BytesUtils.boolToBytes(valueIsNull, valueBytes, 9);

    // Serialize body: actual value if not null
    if (!valueIsNull) {
      serializeValue(seriesDataType, value, valueBytes, 10);
    }
    return valueBytes;
  }

  public static int calcTypeSize(TSDataType dataType, TsPrimitiveType value) {
    return Type.fromTsDataType(dataType).calcTypeSize(value);
  }

  public static boolean isBinaryType(TSDataType dataType) {
    return TEXT == dataType || BLOB == dataType || STRING == dataType;
  }
}
