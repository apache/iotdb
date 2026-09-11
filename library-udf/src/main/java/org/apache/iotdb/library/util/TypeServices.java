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

package org.apache.iotdb.library.util;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.service.TypeService;

import java.io.IOException;

/** Type-specific operations shared by library UDFs. */
public final class TypeServices {

  public static final TypeService<NumericRowReader> NUMERIC_ROW_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> row -> row.getInt(0);
            case INT64 -> row -> row.getLong(0);
            case FLOAT -> row -> row.getFloat(0);
            case DOUBLE -> row -> row.getDouble(0);
            default ->
                row -> {
                  throw new NoNumberException();
                };
          };

  public static final TypeService<IndexedNumericRowReader> INDEXED_NUMERIC_ROW_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> Row::getInt;
            case INT64 -> Row::getLong;
            case FLOAT -> Row::getFloat;
            case DOUBLE -> Row::getDouble;
            default ->
                (row, index) -> {
                  throw new NoNumberException();
                };
          };

  public static final TypeService<NumericRowWriter> NUMERIC_ROW_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> (row, collector) -> collector.putInt(row.getTime(), row.getInt(0));
            case INT64 -> (row, collector) -> collector.putLong(row.getTime(), row.getLong(0));
            case FLOAT -> (row, collector) -> collector.putFloat(row.getTime(), row.getFloat(0));
            case DOUBLE -> (row, collector) -> collector.putDouble(row.getTime(), row.getDouble(0));
            default ->
                (row, collector) -> {
                  throw new NoNumberException();
                };
          };

  public static final TypeService<RowValueReader> ROW_VALUE_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> row -> row.getInt(0);
            case INT64 -> row -> row.getLong(0);
            case FLOAT -> row -> row.getFloat(0);
            case DOUBLE -> row -> row.getDouble(0);
            case BOOLEAN -> row -> row.getBoolean(0);
            case TEXT -> row -> row.getString(0);
            default -> row -> 0;
          };

  public static final TypeService<RowValueWriter> ROW_VALUE_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> (collector, time, value) -> collector.putInt(time, (Integer) value);
            case INT64 -> (collector, time, value) -> collector.putLong(time, (Long) value);
            case FLOAT -> (collector, time, value) -> collector.putFloat(time, (Float) value);
            case DOUBLE -> (collector, time, value) -> collector.putDouble(time, (Double) value);
            case BOOLEAN -> (collector, time, value) -> collector.putBoolean(time, (Boolean) value);
            default -> (collector, time, value) -> {};
          };

  public static final TypeService<ColumnNumericReader> COLUMN_NUMERIC_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> Column::getInt;
            case INT64 -> Column::getLong;
            case FLOAT -> Column::getFloat;
            case DOUBLE -> Column::getDouble;
            case BOOLEAN -> (column, index) -> column.getBoolean(index) ? 1.0D : 0.0D;
            default ->
                (column, index) -> {
                  throw new IllegalArgumentException(
                      "Unsupported data type: " + type.getTypeEnum());
                };
          };

  private static final IndexedNumericRowReader[] INDEXED_NUMERIC_ROW_READERS =
      new IndexedNumericRowReader[Type.values().length];
  private static final RowValueReader[] ROW_VALUE_READERS =
      new RowValueReader[Type.values().length];
  private static final RowValueWriter[] ROW_VALUE_WRITERS =
      new RowValueWriter[Type.values().length];

  static {
    for (Type udfType : Type.values()) {
      org.apache.tsfile.read.common.type.Type readType = toReadType(udfType);
      INDEXED_NUMERIC_ROW_READERS[udfType.ordinal()] =
          INDEXED_NUMERIC_ROW_READER_SERVICE.call(readType);
      ROW_VALUE_READERS[udfType.ordinal()] = ROW_VALUE_READER_SERVICE.call(readType);
      ROW_VALUE_WRITERS[udfType.ordinal()] = ROW_VALUE_WRITER_SERVICE.call(readType);
    }
  }

  public static IndexedNumericRowReader indexedNumericRowReader(Type type) {
    return INDEXED_NUMERIC_ROW_READERS[type.ordinal()];
  }

  public static RowValueReader rowValueReader(Type type) {
    return ROW_VALUE_READERS[type.ordinal()];
  }

  public static RowValueWriter rowValueWriter(Type type) {
    return ROW_VALUE_WRITERS[type.ordinal()];
  }

  public static <T> TypeService<T> numericService(
      T intService, T longService, T floatService, T doubleService, T unsupportedService) {
    return type ->
        switch (type.getTypeEnum()) {
          case INT32 -> intService;
          case INT64 -> longService;
          case FLOAT -> floatService;
          case DOUBLE -> doubleService;
          default -> unsupportedService;
        };
  }

  public static <T> TypeService<T> scalarService(
      T booleanService,
      T intService,
      T longService,
      T floatService,
      T doubleService,
      T textService,
      T unsupportedService) {
    return type ->
        switch (type.getTypeEnum()) {
          case BOOLEAN -> booleanService;
          case INT32 -> intService;
          case INT64 -> longService;
          case FLOAT -> floatService;
          case DOUBLE -> doubleService;
          case TEXT, STRING -> textService;
          default -> unsupportedService;
        };
  }

  public static final TypeService<NumericWindowWriter> NUMERIC_WINDOW_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (times, values, collector) -> {
                  for (int i = 0; i < times.length; i++) {
                    collector.putInt(times[i], (int) Math.round(values[i]));
                  }
                };
            case INT64 ->
                (times, values, collector) -> {
                  for (int i = 0; i < times.length; i++) {
                    collector.putLong(times[i], Math.round(values[i]));
                  }
                };
            case FLOAT ->
                (times, values, collector) -> {
                  for (int i = 0; i < times.length; i++) {
                    collector.putFloat(times[i], (float) values[i]);
                  }
                };
            case DOUBLE ->
                (times, values, collector) -> {
                  for (int i = 0; i < times.length; i++) {
                    collector.putDouble(times[i], values[i]);
                  }
                };
            default ->
                (times, values, collector) -> {
                  throw new NoNumberException();
                };
          };

  public static final TypeService<NumericValueWriter> NUMERIC_VALUE_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> (time, value, collector) -> collector.putInt(time, (int) value);
            case INT64 -> (time, value, collector) -> collector.putLong(time, (long) value);
            case FLOAT -> (time, value, collector) -> collector.putFloat(time, (float) value);
            case DOUBLE -> (time, value, collector) -> collector.putDouble(time, value);
            default ->
                (time, value, collector) -> {
                  throw new NoNumberException();
                };
          };

  public static final TypeService<NumericWindowWriter> NUMERIC_CAST_WINDOW_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (times, values, collector) -> {
                  for (int i = 0; i < times.length; i++) {
                    collector.putInt(times[i], (int) values[i]);
                  }
                };
            case INT64 ->
                (times, values, collector) -> {
                  for (int i = 0; i < times.length; i++) {
                    collector.putLong(times[i], (long) values[i]);
                  }
                };
            case FLOAT ->
                (times, values, collector) -> {
                  for (int i = 0; i < times.length; i++) {
                    collector.putFloat(times[i], (float) values[i]);
                  }
                };
            case DOUBLE ->
                (times, values, collector) -> {
                  for (int i = 0; i < times.length; i++) {
                    collector.putDouble(times[i], values[i]);
                  }
                };
            default ->
                (times, values, collector) -> {
                  throw new NoNumberException();
                };
          };

  public static org.apache.tsfile.read.common.type.Type toReadType(Type type) {
    return org.apache.tsfile.read.common.type.Type.fromTsDataType(
        TSDataType.getTsDataType(type.getType()));
  }

  private TypeServices() {}

  @FunctionalInterface
  public interface NumericRowReader {
    double read(Row row) throws IOException, NoNumberException;
  }

  @FunctionalInterface
  public interface IndexedNumericRowReader {
    double read(Row row, int index) throws IOException, NoNumberException;
  }

  @FunctionalInterface
  public interface NumericRowWriter {
    void write(Row row, PointCollector collector) throws IOException, NoNumberException;
  }

  @FunctionalInterface
  public interface RowValueReader {
    Object read(Row row) throws IOException;
  }

  @FunctionalInterface
  public interface RowValueWriter {
    void write(PointCollector collector, long time, Object value) throws IOException;
  }

  @FunctionalInterface
  public interface ColumnNumericReader {
    double read(Column column, int index);
  }

  @FunctionalInterface
  public interface NumericWindowWriter {
    void write(long[] times, double[] values, PointCollector collector)
        throws IOException, NoNumberException;
  }

  @FunctionalInterface
  public interface NumericValueWriter {
    void write(long time, double value, PointCollector collector)
        throws IOException, NoNumberException;
  }
}
