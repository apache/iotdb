/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.session.util;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDate;

/** Type-specific operations used by the Session record-value wire format. */
final class SessionTypeServices {

  private static final int EMPTY_DATE_INT = 10000101;

  private static final TypeService<ValueLengthCalculator> VALUE_LENGTH_CALCULATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> value -> Byte.BYTES;
            case INT32, DATE -> value -> Integer.BYTES;
            case INT64, TIMESTAMP -> value -> Long.BYTES;
            case FLOAT -> value -> Float.BYTES;
            case DOUBLE -> value -> Double.BYTES;
            case TEXT, STRING, OBJECT -> value -> Integer.BYTES + getTextBytes(value).length;
            case BLOB -> value -> Integer.BYTES + ((Binary) value).getValues().length;
            case ROW, UNKNOWN, VECTOR ->
                value -> {
                  throw unsupportedDataType(type.getTypeEnum());
                };
          };

  private static final TypeService<ValueWriter> VALUE_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> (value, buffer) -> ReadWriteIOUtils.write((Boolean) value, buffer);
            case INT32 -> (value, buffer) -> ReadWriteIOUtils.write((Integer) value, buffer);
            case DATE ->
                (value, buffer) ->
                    ReadWriteIOUtils.write(
                        DateUtils.parseDateExpressionToInt((LocalDate) value), buffer);
            case INT64, TIMESTAMP ->
                (value, buffer) -> ReadWriteIOUtils.write((Long) value, buffer);
            case FLOAT -> (value, buffer) -> ReadWriteIOUtils.write((Float) value, buffer);
            case DOUBLE -> (value, buffer) -> ReadWriteIOUtils.write((Double) value, buffer);
            case TEXT, STRING ->
                (value, buffer) -> {
                  byte[] bytes = getTextBytes(value);
                  ReadWriteIOUtils.write(bytes.length, buffer);
                  buffer.put(bytes);
                };
            case BLOB ->
                (value, buffer) -> {
                  byte[] bytes = ((Binary) value).getValues();
                  ReadWriteIOUtils.write(bytes.length, buffer);
                  buffer.put(bytes);
                };
            // OBJECT was accepted by length calculation historically, but not by value writing.
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (value, buffer) -> {
                  throw unsupportedDataType(type.getTypeEnum());
                };
          };

  private static final TypeService<TabletColumnOccupationCalculator>
      TABLET_COLUMN_OCCUPATION_CALCULATOR_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN -> (values, columnIndex, rowSize) -> rowSize;
                case INT32, FLOAT, DATE ->
                    (values, columnIndex, rowSize) -> rowSize * Integer.BYTES;
                case INT64, DOUBLE, TIMESTAMP ->
                    (values, columnIndex, rowSize) -> rowSize * Long.BYTES;
                case TEXT, BLOB, STRING, OBJECT ->
                    (values, columnIndex, rowSize) -> {
                      int occupation = rowSize * Integer.BYTES;
                      Binary[] binaries = (Binary[]) values[columnIndex];
                      for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
                        occupation +=
                            binaries[rowIndex] != null
                                ? binaries[rowIndex].getLength()
                                : Binary.EMPTY_VALUE.getLength();
                      }
                      return occupation;
                    };
                case ROW, UNKNOWN, VECTOR ->
                    (values, columnIndex, rowSize) -> {
                      throw unsupportedTabletDataType(type.getTypeEnum());
                    };
              };

  private static final TypeService<TabletValueWriter> TABLET_VALUE_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (tablet, columnIndex, valueBuffer) -> {
                  int[] values = (int[]) tablet.getValues()[columnIndex];
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    valueBuffer.putInt(
                        tablet.isNull(index, columnIndex) ? Integer.MIN_VALUE : values[index]);
                  }
                };
            case INT64, TIMESTAMP ->
                (tablet, columnIndex, valueBuffer) -> {
                  long[] values = (long[]) tablet.getValues()[columnIndex];
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    valueBuffer.putLong(
                        tablet.isNull(index, columnIndex) ? Long.MIN_VALUE : values[index]);
                  }
                };
            case FLOAT ->
                (tablet, columnIndex, valueBuffer) -> {
                  float[] values = (float[]) tablet.getValues()[columnIndex];
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    valueBuffer.putFloat(
                        tablet.isNull(index, columnIndex) ? Float.MIN_VALUE : values[index]);
                  }
                };
            case DOUBLE ->
                (tablet, columnIndex, valueBuffer) -> {
                  double[] values = (double[]) tablet.getValues()[columnIndex];
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    valueBuffer.putDouble(
                        tablet.isNull(index, columnIndex) ? Double.MIN_VALUE : values[index]);
                  }
                };
            case BOOLEAN ->
                (tablet, columnIndex, valueBuffer) -> {
                  boolean[] values = (boolean[]) tablet.getValues()[columnIndex];
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    valueBuffer.put(
                        BytesUtils.boolToByte(!tablet.isNull(index, columnIndex) && values[index]));
                  }
                };
            case TEXT, STRING, BLOB, OBJECT ->
                (tablet, columnIndex, valueBuffer) -> {
                  Binary[] values = (Binary[]) tablet.getValues()[columnIndex];
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    Binary value =
                        !tablet.isNull(index, columnIndex) && values[index] != null
                            ? values[index]
                            : Binary.EMPTY_VALUE;
                    valueBuffer.putInt(value.getLength());
                    valueBuffer.put(value.getValues());
                  }
                };
            case DATE ->
                (tablet, columnIndex, valueBuffer) -> {
                  LocalDate[] values = (LocalDate[]) tablet.getValues()[columnIndex];
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    valueBuffer.putInt(
                        !tablet.isNull(index, columnIndex) && values[index] != null
                            ? DateUtils.parseDateExpressionToInt(values[index])
                            : EMPTY_DATE_INT);
                  }
                };
            case ROW, UNKNOWN, VECTOR ->
                (tablet, columnIndex, valueBuffer) -> {
                  throw unsupportedTabletDataType(type.getTypeEnum());
                };
          };

  private static final TypeService<TabletValueEncoder> TABLET_VALUE_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (tablet, columnIndex, encoder, outputStream) -> {
                  int[] values = (int[]) tablet.getValues()[columnIndex];
                  int lastNonNullValue = 0;
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    if (!tablet.isNull(index, columnIndex)) {
                      lastNonNullValue = values[index];
                    }
                    encoder.encode(lastNonNullValue, outputStream);
                  }
                };
            case INT64, TIMESTAMP ->
                (tablet, columnIndex, encoder, outputStream) -> {
                  long[] values = (long[]) tablet.getValues()[columnIndex];
                  long lastNonNullValue = 0;
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    if (!tablet.isNull(index, columnIndex)) {
                      lastNonNullValue = values[index];
                    }
                    encoder.encode(lastNonNullValue, outputStream);
                  }
                };
            case FLOAT ->
                (tablet, columnIndex, encoder, outputStream) -> {
                  float[] values = (float[]) tablet.getValues()[columnIndex];
                  float lastNonNullValue = 0.0f;
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    if (!tablet.isNull(index, columnIndex)) {
                      lastNonNullValue = values[index];
                    }
                    encoder.encode(lastNonNullValue, outputStream);
                  }
                };
            case DOUBLE ->
                (tablet, columnIndex, encoder, outputStream) -> {
                  double[] values = (double[]) tablet.getValues()[columnIndex];
                  double lastNonNullValue = 0.0;
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    if (!tablet.isNull(index, columnIndex)) {
                      lastNonNullValue = values[index];
                    }
                    encoder.encode(lastNonNullValue, outputStream);
                  }
                };
            case BOOLEAN ->
                (tablet, columnIndex, encoder, outputStream) -> {
                  boolean[] values = (boolean[]) tablet.getValues()[columnIndex];
                  boolean lastNonNullValue = false;
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    if (!tablet.isNull(index, columnIndex)) {
                      lastNonNullValue = values[index];
                    }
                    encoder.encode(lastNonNullValue, outputStream);
                  }
                };
            case TEXT, STRING, BLOB ->
                (tablet, columnIndex, encoder, outputStream) -> {
                  Binary[] values = (Binary[]) tablet.getValues()[columnIndex];
                  Binary lastNonNullValue = Binary.EMPTY_VALUE;
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    if (!tablet.isNull(index, columnIndex) && values[index] != null) {
                      lastNonNullValue = values[index];
                    }
                    encoder.encode(lastNonNullValue, outputStream);
                  }
                };
            case DATE ->
                (tablet, columnIndex, encoder, outputStream) -> {
                  LocalDate[] values = (LocalDate[]) tablet.getValues()[columnIndex];
                  int lastNonNullValue = EMPTY_DATE_INT;
                  for (int index = 0; index < tablet.getRowSize(); index++) {
                    if (!tablet.isNull(index, columnIndex)) {
                      lastNonNullValue = DateUtils.parseDateExpressionToInt(values[index]);
                    }
                    // Previous values make null runs more compressible without changing the bitmap.
                    encoder.encode(lastNonNullValue, outputStream);
                  }
                };
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (tablet, columnIndex, encoder, outputStream) -> {
                  throw unsupportedTabletDataType(type.getTypeEnum());
                };
          };

  private static final TypeService<ValueListSorter> VALUE_LIST_SORTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (valueList, index) -> {
                  boolean[] values = (boolean[]) valueList;
                  boolean[] sortedValues = new boolean[values.length];
                  for (int i = 0; i < index.length; i++) {
                    sortedValues[i] = values[index[i]];
                  }
                  return sortedValues;
                };
            case INT32 ->
                (valueList, index) -> {
                  int[] values = (int[]) valueList;
                  int[] sortedValues = new int[values.length];
                  for (int i = 0; i < index.length; i++) {
                    sortedValues[i] = values[index[i]];
                  }
                  return sortedValues;
                };
            case DATE ->
                (valueList, index) -> {
                  LocalDate[] values = (LocalDate[]) valueList;
                  LocalDate[] sortedValues = new LocalDate[values.length];
                  for (int i = 0; i < index.length; i++) {
                    sortedValues[i] = values[index[i]];
                  }
                  return sortedValues;
                };
            case INT64, TIMESTAMP ->
                (valueList, index) -> {
                  long[] values = (long[]) valueList;
                  long[] sortedValues = new long[values.length];
                  for (int i = 0; i < index.length; i++) {
                    sortedValues[i] = values[index[i]];
                  }
                  return sortedValues;
                };
            case FLOAT ->
                (valueList, index) -> {
                  float[] values = (float[]) valueList;
                  float[] sortedValues = new float[values.length];
                  for (int i = 0; i < index.length; i++) {
                    sortedValues[i] = values[index[i]];
                  }
                  return sortedValues;
                };
            case DOUBLE ->
                (valueList, index) -> {
                  double[] values = (double[]) valueList;
                  double[] sortedValues = new double[values.length];
                  for (int i = 0; i < index.length; i++) {
                    sortedValues[i] = values[index[i]];
                  }
                  return sortedValues;
                };
            case TEXT, BLOB, STRING, OBJECT ->
                (valueList, index) -> {
                  Binary[] values = (Binary[]) valueList;
                  Binary[] sortedValues = new Binary[values.length];
                  for (int i = 0; i < index.length; i++) {
                    sortedValues[i] = values[index[i]];
                  }
                  return sortedValues;
                };
            case ROW, UNKNOWN, VECTOR ->
                (valueList, index) -> {
                  throw unsupportedValueListDataType(type.getTypeEnum());
                };
          };

  static {
    VALUE_LENGTH_CALCULATOR_SERVICE.check();
    VALUE_WRITER_SERVICE.check();
    TABLET_COLUMN_OCCUPATION_CALCULATOR_SERVICE.check();
    TABLET_VALUE_WRITER_SERVICE.check();
    TABLET_VALUE_ENCODER_SERVICE.check();
    VALUE_LIST_SORTER_SERVICE.check();
  }

  private SessionTypeServices() {}

  static ValueLengthCalculator valueLengthCalculator(TSDataType dataType) {
    return VALUE_LENGTH_CALCULATOR_SERVICE.call(Type.fromTsDataType(dataType));
  }

  static ValueWriter valueWriter(TSDataType dataType) {
    return VALUE_WRITER_SERVICE.call(Type.fromTsDataType(dataType));
  }

  static TabletColumnOccupationCalculator tabletColumnOccupationCalculator(TSDataType dataType) {
    return TABLET_COLUMN_OCCUPATION_CALCULATOR_SERVICE.call(Type.fromTsDataType(dataType));
  }

  static TabletValueWriter tabletValueWriter(TSDataType dataType) {
    return TABLET_VALUE_WRITER_SERVICE.call(Type.fromTsDataType(dataType));
  }

  static TabletValueEncoder tabletValueEncoder(TSDataType dataType) {
    return TABLET_VALUE_ENCODER_SERVICE.call(Type.fromTsDataType(dataType));
  }

  static ValueListSorter valueListSorter(TSDataType dataType) {
    return VALUE_LIST_SORTER_SERVICE.call(Type.fromTsDataType(dataType));
  }

  private static byte[] getTextBytes(Object value) {
    if (value instanceof Binary binary) {
      return binary.getValues();
    }
    return ((String) value).getBytes(TSFileConfig.STRING_CHARSET);
  }

  private static IoTDBConnectionException unsupportedDataType(Object dataType) {
    return new IoTDBConnectionException(Session.MSG_UNSUPPORTED_DATA_TYPE + dataType);
  }

  private static UnSupportedDataTypeException unsupportedTabletDataType(Object dataType) {
    return new UnSupportedDataTypeException(
        String.format("Data type %s is not supported.", dataType));
  }

  private static UnSupportedDataTypeException unsupportedValueListDataType(Object dataType) {
    return new UnSupportedDataTypeException(Session.MSG_UNSUPPORTED_DATA_TYPE + dataType);
  }

  @FunctionalInterface
  interface ValueLengthCalculator {
    int calculate(Object value) throws IoTDBConnectionException;
  }

  @FunctionalInterface
  interface ValueWriter {
    void write(Object value, ByteBuffer buffer) throws IoTDBConnectionException;
  }

  @FunctionalInterface
  interface TabletColumnOccupationCalculator {
    int calculate(Object[] values, int columnIndex, int rowSize);
  }

  @FunctionalInterface
  interface TabletValueWriter {
    void write(Tablet tablet, int columnIndex, ByteBuffer valueBuffer);
  }

  @FunctionalInterface
  interface TabletValueEncoder {
    void encode(
        Tablet tablet, int columnIndex, Encoder encoder, ByteArrayOutputStream outputStream);
  }

  @FunctionalInterface
  interface ValueListSorter {
    Object sort(Object valueList, int[] index);
  }
}
