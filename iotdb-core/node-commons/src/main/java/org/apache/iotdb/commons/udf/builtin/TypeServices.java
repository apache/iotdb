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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;

import org.apache.tsfile.read.common.type.service.TypeService;

import java.io.IOException;

/** Type-specific operations used by numeric built-in UDFs. */
final class TypeServices {

  // Select the primitive Row accessor once per UDF instance instead of switching for every row.
  static final TypeService<PreviousValueReader> VALUE_TREND_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> (target, row) -> target.previousInt = row.getInt(0);
            case INT64 -> (target, row) -> target.previousLong = row.getLong(0);
            case FLOAT -> (target, row) -> target.previousFloat = row.getFloat(0);
            case DOUBLE -> (target, row) -> target.previousDouble = row.getDouble(0);
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (target, row) -> {
                  throw target.invalidDataType();
                };
          };

  static final TypeService<ValueDifferenceOperator> VALUE_DIFFERENCE_OPERATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (target, time, row, collector) -> {
                  int current = row.getInt(0);
                  collector.putInt(time, current - target.previousInt);
                  target.previousInt = current;
                };
            case INT64 ->
                (target, time, row, collector) -> {
                  long current = row.getLong(0);
                  collector.putLong(time, current - target.previousLong);
                  target.previousLong = current;
                };
            case FLOAT ->
                (target, time, row, collector) -> {
                  float current = row.getFloat(0);
                  collector.putFloat(time, current - target.previousFloat);
                  target.previousFloat = current;
                };
            case DOUBLE ->
                (target, time, row, collector) -> {
                  double current = row.getDouble(0);
                  collector.putDouble(time, current - target.previousDouble);
                  target.previousDouble = current;
                };
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (target, time, row, collector) -> {
                  throw target.invalidDataType();
                };
          };

  static final TypeService<ValueDifferenceOperator> NON_NEGATIVE_VALUE_DIFFERENCE_OPERATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (target, time, row, collector) -> {
                  int current = row.getInt(0);
                  collector.putInt(time, Math.abs(current - target.previousInt));
                  target.previousInt = current;
                };
            case INT64 ->
                (target, time, row, collector) -> {
                  long current = row.getLong(0);
                  collector.putLong(time, Math.abs(current - target.previousLong));
                  target.previousLong = current;
                };
            case FLOAT ->
                (target, time, row, collector) -> {
                  float current = row.getFloat(0);
                  collector.putFloat(time, Math.abs(current - target.previousFloat));
                  target.previousFloat = current;
                };
            case DOUBLE ->
                (target, time, row, collector) -> {
                  double current = row.getDouble(0);
                  collector.putDouble(time, Math.abs(current - target.previousDouble));
                  target.previousDouble = current;
                };
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (target, time, row, collector) -> {
                  throw target.invalidDataType();
                };
          };

  static final TypeService<DerivativeOperator> DERIVATIVE_OPERATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (target, time, row, collector, timeDelta) -> {
                  int current = row.getInt(0);
                  collector.putDouble(time, (current - target.previousInt) / timeDelta);
                  target.previousInt = current;
                };
            case INT64 ->
                (target, time, row, collector, timeDelta) -> {
                  long current = row.getLong(0);
                  collector.putDouble(time, (current - target.previousLong) / timeDelta);
                  target.previousLong = current;
                };
            case FLOAT ->
                (target, time, row, collector, timeDelta) -> {
                  float current = row.getFloat(0);
                  collector.putDouble(time, (current - target.previousFloat) / timeDelta);
                  target.previousFloat = current;
                };
            case DOUBLE ->
                (target, time, row, collector, timeDelta) -> {
                  double current = row.getDouble(0);
                  collector.putDouble(time, (current - target.previousDouble) / timeDelta);
                  target.previousDouble = current;
                };
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (target, time, row, collector, timeDelta) -> {
                  throw target.invalidDataType();
                };
          };

  static final TypeService<DerivativeOperator> NON_NEGATIVE_DERIVATIVE_OPERATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (target, time, row, collector, timeDelta) -> {
                  int current = row.getInt(0);
                  collector.putDouble(time, Math.abs(current - target.previousInt) / timeDelta);
                  target.previousInt = current;
                };
            case INT64 ->
                (target, time, row, collector, timeDelta) -> {
                  long current = row.getLong(0);
                  collector.putDouble(time, Math.abs(current - target.previousLong) / timeDelta);
                  target.previousLong = current;
                };
            case FLOAT ->
                (target, time, row, collector, timeDelta) -> {
                  float current = row.getFloat(0);
                  collector.putDouble(time, Math.abs(current - target.previousFloat) / timeDelta);
                  target.previousFloat = current;
                };
            case DOUBLE ->
                (target, time, row, collector, timeDelta) -> {
                  double current = row.getDouble(0);
                  collector.putDouble(time, Math.abs(current - target.previousDouble) / timeDelta);
                  target.previousDouble = current;
                };
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (target, time, row, collector, timeDelta) -> {
                  throw target.invalidDataType();
                };
          };

  static {
    VALUE_TREND_READER_SERVICE.check();
    VALUE_DIFFERENCE_OPERATOR_SERVICE.check();
    NON_NEGATIVE_VALUE_DIFFERENCE_OPERATOR_SERVICE.check();
    DERIVATIVE_OPERATOR_SERVICE.check();
    NON_NEGATIVE_DERIVATIVE_OPERATOR_SERVICE.check();
  }

  private TypeServices() {}

  @FunctionalInterface
  interface PreviousValueReader {
    void read(UDTFValueTrend target, Row row)
        throws UDFInputSeriesDataTypeNotValidException, IOException;
  }

  @FunctionalInterface
  interface ValueDifferenceOperator {
    void apply(UDTFValueTrend target, long time, Row row, PointCollector collector)
        throws UDFInputSeriesDataTypeNotValidException, IOException;
  }

  @FunctionalInterface
  interface DerivativeOperator {
    void apply(
        UDTFValueTrend target, long time, Row row, PointCollector collector, double timeDelta)
        throws UDFInputSeriesDataTypeNotValidException, IOException;
  }
}
