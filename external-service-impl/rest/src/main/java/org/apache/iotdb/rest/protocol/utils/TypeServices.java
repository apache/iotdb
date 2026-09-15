/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.rest.protocol.utils;

import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.rest.i18n.RestMessages;

import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;

import java.nio.charset.StandardCharsets;

/** Type-specific column writers used by the REST insert handlers. */
public final class TypeServices {

  public static final TypeService<ColumnWriter> INSERT_TABLET_COLUMN_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> TypeServices::writeBooleanColumn;
            case INT32, DATE -> TypeServices::writeIntColumn;
            case INT64, TIMESTAMP -> TypeServices::writeLongColumn;
            case FLOAT -> TypeServices::writeFloatColumn;
            case DOUBLE -> TypeServices::writeDoubleColumn;
            case TEXT, BLOB, STRING -> TypeServices::writeBinaryColumn;
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (valueAccessor, rowCount, bitMap) -> {
                  throw new IllegalArgumentException(
                      RestMessages.INVALID_INPUT + type.getTypeEnum());
                };
          };

  static {
    INSERT_TABLET_COLUMN_WRITER_SERVICE.check();
  }

  private static Object writeBooleanColumn(
      ValueAccessor valueAccessor, int rowCount, BitMap bitMap) {
    boolean[] values = new boolean[rowCount];
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      Object data = valueAccessor.get(rowIndex);
      if (data == null) {
        bitMap.mark(rowIndex);
      } else if ("1".equals(data.toString())) {
        values[rowIndex] = true;
      } else if ("0".equals(data.toString())) {
        values[rowIndex] = false;
      } else {
        values[rowIndex] = (Boolean) data;
      }
    }
    return values;
  }

  private static Object writeIntColumn(ValueAccessor valueAccessor, int rowCount, BitMap bitMap)
      throws WriteProcessRejectException {
    int[] values = new int[rowCount];
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      Object data = valueAccessor.get(rowIndex);
      if (data == null) {
        bitMap.mark(rowIndex);
      } else if (data instanceof Integer) {
        values[rowIndex] = (int) data;
      } else {
        throw unsupportedValue(data);
      }
    }
    return values;
  }

  private static Object writeLongColumn(ValueAccessor valueAccessor, int rowCount, BitMap bitMap)
      throws WriteProcessRejectException {
    long[] values = new long[rowCount];
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      Object data = valueAccessor.get(rowIndex);
      if (data == null) {
        bitMap.mark(rowIndex);
      } else if (data instanceof Integer) {
        values[rowIndex] = (int) data;
      } else if (data instanceof Long) {
        values[rowIndex] = (long) data;
      } else {
        throw unsupportedValue(data);
      }
    }
    return values;
  }

  private static Object writeFloatColumn(ValueAccessor valueAccessor, int rowCount, BitMap bitMap) {
    float[] values = new float[rowCount];
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      Object data = valueAccessor.get(rowIndex);
      if (data == null) {
        bitMap.mark(rowIndex);
      } else {
        values[rowIndex] = Float.parseFloat(String.valueOf(data));
      }
    }
    return values;
  }

  private static Object writeDoubleColumn(
      ValueAccessor valueAccessor, int rowCount, BitMap bitMap) {
    double[] values = new double[rowCount];
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      Object data = valueAccessor.get(rowIndex);
      if (data == null) {
        bitMap.mark(rowIndex);
      } else {
        values[rowIndex] = Double.parseDouble(String.valueOf(data));
      }
    }
    return values;
  }

  private static Object writeBinaryColumn(
      ValueAccessor valueAccessor, int rowCount, BitMap bitMap) {
    Binary[] values = new Binary[rowCount];
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      Object data = valueAccessor.get(rowIndex);
      if (data == null) {
        bitMap.mark(rowIndex);
        values[rowIndex] = new Binary(new byte[0]);
      } else {
        values[rowIndex] = new Binary(data.toString().getBytes(StandardCharsets.UTF_8));
      }
    }
    return values;
  }

  private static WriteProcessRejectException unsupportedValue(Object value) {
    return new WriteProcessRejectException("unsupported data type: " + value.getClass());
  }

  @FunctionalInterface
  public interface ValueAccessor {
    Object get(int rowIndex);
  }

  @FunctionalInterface
  public interface ColumnWriter {
    Object write(ValueAccessor valueAccessor, int rowCount, BitMap bitMap)
        throws WriteProcessRejectException;
  }

  private TypeServices() {}
}
