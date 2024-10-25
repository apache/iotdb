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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static org.apache.tsfile.enums.TSDataType.BLOB;
import static org.apache.tsfile.enums.TSDataType.STRING;
import static org.apache.tsfile.enums.TSDataType.TEXT;

public class Utils {
  public static final String UNSUPPORTED_TYPE_MESSAGE = "Unsupported data type : %s";

  private Utils() {}

  public static void serializeValue(
      TSDataType dataType, TsPrimitiveType value, byte[] valueBytes, int offset) {
    switch (dataType) {
      case INT32:
      case DATE:
        BytesUtils.intToBytes(value.getInt(), valueBytes, offset);
        break;
      case INT64:
      case TIMESTAMP:
        BytesUtils.longToBytes(value.getLong(), valueBytes, offset);
        break;
      case FLOAT:
        BytesUtils.floatToBytes(value.getFloat(), valueBytes, offset);
        break;
      case DOUBLE:
        BytesUtils.doubleToBytes(value.getDouble(), valueBytes, offset);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        BytesUtils.intToBytes(value.getBinary().getValues().length, valueBytes, offset);
        offset += 4;
        System.arraycopy(
            value.getBinary().getValues(),
            0,
            valueBytes,
            offset,
            value.getBinary().getValues().length);
        break;
      case BOOLEAN:
        BytesUtils.boolToBytes(value.getBoolean(), valueBytes, offset);
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(UNSUPPORTED_TYPE_MESSAGE, dataType));
    }
  }

  public static void serializeBinaryValue(Binary binary, byte[] valueBytes, int offset) {
    BytesUtils.intToBytes(binary.getValues().length, valueBytes, offset);
    offset += Integer.BYTES;
    System.arraycopy(binary.getValues(), 0, valueBytes, offset, binary.getValues().length);
  }

  public static byte[] serializeTimeValue(
      TSDataType seriesDataType, long time, TsPrimitiveType value) {
    byte[] valueBytes = new byte[8 + calcTypeSize(seriesDataType, value)];
    BytesUtils.longToBytes(time, valueBytes, 0);
    serializeValue(seriesDataType, value, valueBytes, 8);
    return valueBytes;
  }

  public static byte[] serializeTimeValue(
      TSDataType seriesDataType, long time, boolean valueIsNull, TsPrimitiveType value) {
    byte[] valueBytes;
    if (valueIsNull) {
      valueBytes = new byte[9];
      BytesUtils.longToBytes(time, valueBytes, 0);
      BytesUtils.boolToBytes(true, valueBytes, 8);
      return valueBytes;
    }

    valueBytes = new byte[9 + calcTypeSize(seriesDataType, value)];
    BytesUtils.longToBytes(time, valueBytes, 0);
    BytesUtils.boolToBytes(false, valueBytes, 8);
    serializeValue(seriesDataType, value, valueBytes, 9);
    return valueBytes;
  }

  public static int calcTypeSize(TSDataType dataType, TsPrimitiveType value) {
    switch (dataType) {
      case BOOLEAN:
        return 1;
      case INT32:
      case DATE:
      case FLOAT:
        return 4;
      case INT64:
      case TIMESTAMP:
      case DOUBLE:
        return 8;
      case TEXT:
      case BLOB:
      case STRING:
        return 4 + value.getBinary().getValues().length;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", dataType));
    }
  }

  public static boolean isBinaryType(TSDataType dataType) {
    return TEXT == dataType || BLOB == dataType || STRING == dataType;
  }
}
