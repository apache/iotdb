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

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
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

  // UDF Row has no generic numeric accessor, so bind its primitive getter once during beforeStart.
  static final TypeService<NumericRowReader> NUMERIC_ROW_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> row -> row.getInt(0);
            case INT64 -> row -> row.getLong(0);
            case FLOAT -> row -> row.getFloat(0);
            case DOUBLE -> row -> row.getDouble(0);
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                row -> {
                  throw invalidNumericDataType(type);
                };
          };

  // TsFile Type provides primitive numeric conversion for every supported column implementation.
  static final TypeService<NumericColumnReader> NUMERIC_COLUMN_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, INT64, FLOAT, DOUBLE -> type::getDouble;
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (column, position) -> {
                  throw invalidNumericDataType(type);
                };
          };

  static final TypeService<NumericRowCollector> NUMERIC_ROW_COLLECTOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> (row, collector) -> collector.putInt(row.getTime(), row.getInt(0));
            case INT64 -> (row, collector) -> collector.putLong(row.getTime(), row.getLong(0));
            case FLOAT -> (row, collector) -> collector.putFloat(row.getTime(), row.getFloat(0));
            case DOUBLE -> (row, collector) -> collector.putDouble(row.getTime(), row.getDouble(0));
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (row, collector) -> {
                  throw invalidNumericDataType(type);
                };
          };

  static final TypeService<AbsRowCollector> ABS_ROW_COLLECTOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (row, collector) -> collector.putInt(row.getTime(), Math.abs(row.getInt(0)));
            case INT64 ->
                (row, collector) -> collector.putLong(row.getTime(), Math.abs(row.getLong(0)));
            case FLOAT ->
                (row, collector) -> collector.putFloat(row.getTime(), Math.abs(row.getFloat(0)));
            case DOUBLE ->
                (row, collector) -> collector.putDouble(row.getTime(), Math.abs(row.getDouble(0)));
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (row, collector) -> {
                  throw invalidNumericDataType(type);
                };
          };

  static final TypeService<AbsRowMapper> ABS_ROW_MAPPER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> row -> Math.abs(row.getInt(0));
            case INT64 -> row -> Math.abs(row.getLong(0));
            case FLOAT -> row -> Math.abs(row.getFloat(0));
            case DOUBLE -> row -> Math.abs(row.getDouble(0));
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                row -> {
                  throw invalidNumericDataType(type);
                };
          };

  static final TypeService<AbsColumnTransformer> ABS_COLUMN_TRANSFORMER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> UDTFAbs::transformInt;
            case INT64 -> UDTFAbs::transformLong;
            case FLOAT -> UDTFAbs::transformFloat;
            case DOUBLE -> UDTFAbs::transformDouble;
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (target, columns, builder) -> {
                  throw invalidNumericDataType(type);
                };
          };

  static final TypeService<NumericWindowTransformer<UDTFM4>> M4_WINDOW_TRANSFORMER_SERVICE =
      numericWindowTransformerService(
          UDTFM4::transformInt,
          UDTFM4::transformLong,
          UDTFM4::transformFloat,
          UDTFM4::transformDouble);

  static final TypeService<NumericWindowTransformer<UDTFEqualSizeBucketM4Sample>>
      BUCKET_M4_WINDOW_TRANSFORMER_SERVICE =
          numericWindowTransformerService(
              UDTFEqualSizeBucketM4Sample::transformInt,
              UDTFEqualSizeBucketM4Sample::transformLong,
              UDTFEqualSizeBucketM4Sample::transformFloat,
              UDTFEqualSizeBucketM4Sample::transformDouble);

  static final TypeService<NumericWindowTransformer<UDTFEqualSizeBucketAggSample>>
      BUCKET_AGG_WINDOW_TRANSFORMER_SERVICE =
          numericWindowTransformerService(
              UDTFEqualSizeBucketAggSample::aggregateInt,
              UDTFEqualSizeBucketAggSample::aggregateLong,
              UDTFEqualSizeBucketAggSample::aggregateFloat,
              UDTFEqualSizeBucketAggSample::aggregateDouble);

  static final TypeService<NumericWindowTransformer<UDTFEqualSizeBucketOutlierSample>>
      BUCKET_OUTLIER_WINDOW_TRANSFORMER_SERVICE =
          numericWindowTransformerService(
              UDTFEqualSizeBucketOutlierSample::outlierSampleInt,
              UDTFEqualSizeBucketOutlierSample::outlierSampleLong,
              UDTFEqualSizeBucketOutlierSample::outlierSampleFloat,
              UDTFEqualSizeBucketOutlierSample::outlierSampleDouble);

  static final TypeService<ChangePointProcessor> CHANGE_POINT_PROCESSOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> UDTFChangePoints::transformBoolean;
            case INT32 -> UDTFChangePoints::transformInt;
            case INT64 -> UDTFChangePoints::transformLong;
            case FLOAT -> UDTFChangePoints::transformFloat;
            case DOUBLE -> UDTFChangePoints::transformDouble;
            case TEXT -> UDTFChangePoints::transformString;
            case DATE, TIMESTAMP, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (target, row, collector) -> {};
          };

  static final TypeService<SelectKRowTransformer> SELECT_K_ROW_TRANSFORMER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> (target, row) -> target.transformInt(row.getTime(), row.getInt(0));
            case INT64, TIMESTAMP ->
                (target, row) -> target.transformLong(row.getTime(), row.getLong(0));
            case FLOAT -> (target, row) -> target.transformFloat(row.getTime(), row.getFloat(0));
            case DOUBLE -> (target, row) -> target.transformDouble(row.getTime(), row.getDouble(0));
            case TEXT, STRING ->
                (target, row) -> target.transformString(row.getTime(), row.getString(0));
            case BOOLEAN, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (target, row) -> {
                  throw invalidSelectKDataType(type);
                };
          };

  static final TypeService<SelectKTerminator> SELECT_K_TERMINATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> UDTFSelectK::terminateInt;
            case INT64, TIMESTAMP -> UDTFSelectK::terminateLong;
            case FLOAT -> UDTFSelectK::terminateFloat;
            case DOUBLE -> UDTFSelectK::terminateDouble;
            case TEXT, STRING -> UDTFSelectK::terminateString;
            case BOOLEAN, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (target, collector) -> {
                  throw invalidSelectKDataType(type);
                };
          };

  static final TypeService<TopKQueueConstructor> TOP_K_QUEUE_CONSTRUCTOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> UDTFTopK::initializeIntQueue;
            case INT64, TIMESTAMP -> UDTFTopK::initializeLongQueue;
            case FLOAT -> UDTFTopK::initializeFloatQueue;
            case DOUBLE -> UDTFTopK::initializeDoubleQueue;
            case TEXT, STRING -> UDTFTopK::initializeStringQueue;
            case BOOLEAN, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                target -> {
                  throw invalidSelectKDataType(type);
                };
          };

  static final TypeService<BottomKQueueConstructor> BOTTOM_K_QUEUE_CONSTRUCTOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> UDTFBottomK::initializeIntQueue;
            case INT64, TIMESTAMP -> UDTFBottomK::initializeLongQueue;
            case FLOAT -> UDTFBottomK::initializeFloatQueue;
            case DOUBLE -> UDTFBottomK::initializeDoubleQueue;
            case TEXT, STRING -> UDTFBottomK::initializeStringQueue;
            case BOOLEAN, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                target -> {
                  throw invalidSelectKDataType(type);
                };
          };

  static final TypeService<ConstantParser> CONSTANT_PARSER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> UDTFConst::parseInt;
            case DATE -> UDTFConst::parseDate;
            case INT64, TIMESTAMP -> UDTFConst::parseLong;
            case FLOAT -> UDTFConst::parseFloat;
            case DOUBLE -> UDTFConst::parseDouble;
            case BOOLEAN -> UDTFConst::parseBoolean;
            case TEXT, STRING -> UDTFConst::parseText;
            case BLOB, OBJECT -> UDTFConst::parseBlob;
            case ROW, UNKNOWN, VECTOR ->
                (target, parameters) -> {
                  throw new UnsupportedOperationException();
                };
          };

  static final TypeService<ConstantRowCollector> CONSTANT_ROW_COLLECTOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (target, row, collector) -> collector.putInt(row.getTime(), target.intValue());
            case INT64, TIMESTAMP ->
                (target, row, collector) -> collector.putLong(row.getTime(), target.longValue());
            case FLOAT ->
                (target, row, collector) -> collector.putFloat(row.getTime(), target.floatValue());
            case DOUBLE ->
                (target, row, collector) ->
                    collector.putDouble(row.getTime(), target.doubleValue());
            case BOOLEAN ->
                (target, row, collector) ->
                    collector.putBoolean(row.getTime(), target.booleanValue());
            case TEXT, STRING, BLOB, OBJECT ->
                (target, row, collector) ->
                    collector.putBinary(row.getTime(), target.binaryValue());
            case ROW, UNKNOWN, VECTOR ->
                (target, row, collector) -> {
                  throw new UnsupportedOperationException();
                };
          };

  static final TypeService<ConstantRowMapper> CONSTANT_ROW_MAPPER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> UDTFConst::intValue;
            case INT64, TIMESTAMP -> UDTFConst::longValue;
            case FLOAT -> UDTFConst::floatValue;
            case DOUBLE -> UDTFConst::doubleValue;
            case BOOLEAN -> UDTFConst::booleanValue;
            case TEXT, STRING, BLOB, OBJECT -> UDTFConst::binaryValue;
            case ROW, UNKNOWN, VECTOR ->
                target -> {
                  throw new UnsupportedOperationException();
                };
          };

  static final TypeService<ConstantColumnValueWriter> CONSTANT_COLUMN_VALUE_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> (target, builder) -> builder.writeInt(target.intValue());
            case INT64, TIMESTAMP -> (target, builder) -> builder.writeLong(target.longValue());
            case FLOAT -> (target, builder) -> builder.writeFloat(target.floatValue());
            case DOUBLE -> (target, builder) -> builder.writeDouble(target.doubleValue());
            case BOOLEAN -> (target, builder) -> builder.writeBoolean(target.booleanValue());
            case TEXT, STRING, BLOB, OBJECT ->
                (target, builder) -> builder.writeBinary(target.tsFileBinaryValue());
            case ROW, UNKNOWN, VECTOR ->
                (target, builder) -> {
                  throw new UnsupportedOperationException();
                };
          };

  static final TypeService<ContinuouslySatisfyRowTransformer>
      CONTINUOUSLY_SATISFY_ROW_TRANSFORMER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32 -> (target, row) -> target.transformInt(row.getTime(), row.getInt(0));
                case INT64 -> (target, row) -> target.transformLong(row.getTime(), row.getLong(0));
                case FLOAT ->
                    (target, row) -> target.transformFloat(row.getTime(), row.getFloat(0));
                case DOUBLE ->
                    (target, row) -> target.transformDouble(row.getTime(), row.getDouble(0));
                case BOOLEAN ->
                    (target, row) -> target.transformBoolean(row.getTime(), row.getBoolean(0));
                case DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                    (target, row) -> {
                      throw invalidContinuouslySatisfyDataType(type);
                    };
              };

  static final TypeService<ContinuouslySatisfyTerminator> CONTINUOUSLY_SATISFY_TERMINATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, INT64, FLOAT, DOUBLE, BOOLEAN ->
                UDTFContinuouslySatisfy::terminateSupportedType;
            case DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (target, collector) -> {
                  throw invalidContinuouslySatisfyDataType(type);
                };
          };

  static {
    VALUE_TREND_READER_SERVICE.check();
    VALUE_DIFFERENCE_OPERATOR_SERVICE.check();
    NON_NEGATIVE_VALUE_DIFFERENCE_OPERATOR_SERVICE.check();
    DERIVATIVE_OPERATOR_SERVICE.check();
    NON_NEGATIVE_DERIVATIVE_OPERATOR_SERVICE.check();
    NUMERIC_ROW_READER_SERVICE.check();
    NUMERIC_COLUMN_READER_SERVICE.check();
    NUMERIC_ROW_COLLECTOR_SERVICE.check();
    ABS_ROW_COLLECTOR_SERVICE.check();
    ABS_ROW_MAPPER_SERVICE.check();
    ABS_COLUMN_TRANSFORMER_SERVICE.check();
    M4_WINDOW_TRANSFORMER_SERVICE.check();
    BUCKET_M4_WINDOW_TRANSFORMER_SERVICE.check();
    BUCKET_AGG_WINDOW_TRANSFORMER_SERVICE.check();
    BUCKET_OUTLIER_WINDOW_TRANSFORMER_SERVICE.check();
    CHANGE_POINT_PROCESSOR_SERVICE.check();
    SELECT_K_ROW_TRANSFORMER_SERVICE.check();
    SELECT_K_TERMINATOR_SERVICE.check();
    TOP_K_QUEUE_CONSTRUCTOR_SERVICE.check();
    BOTTOM_K_QUEUE_CONSTRUCTOR_SERVICE.check();
    CONSTANT_PARSER_SERVICE.check();
    CONSTANT_ROW_COLLECTOR_SERVICE.check();
    CONSTANT_ROW_MAPPER_SERVICE.check();
    CONSTANT_COLUMN_VALUE_WRITER_SERVICE.check();
    CONTINUOUSLY_SATISFY_ROW_TRANSFORMER_SERVICE.check();
    CONTINUOUSLY_SATISFY_TERMINATOR_SERVICE.check();
  }

  private TypeServices() {}

  private static UDFInputSeriesDataTypeNotValidException invalidNumericDataType(
      org.apache.tsfile.read.common.type.Type type) {
    return new UDFInputSeriesDataTypeNotValidException(
        0,
        UDFDataTypeTransformer.transformReadTypeToUDFDataType(type),
        Type.INT32,
        Type.INT64,
        Type.FLOAT,
        Type.DOUBLE);
  }

  private static UDFInputSeriesDataTypeNotValidException invalidSelectKDataType(
      org.apache.tsfile.read.common.type.Type type) {
    return new UDFInputSeriesDataTypeNotValidException(
        0,
        UDFDataTypeTransformer.transformReadTypeToUDFDataType(type),
        Type.INT32,
        Type.INT64,
        Type.FLOAT,
        Type.DOUBLE,
        Type.TEXT,
        Type.DATE,
        Type.TIMESTAMP,
        Type.STRING);
  }

  private static UDFInputSeriesDataTypeNotValidException invalidContinuouslySatisfyDataType(
      org.apache.tsfile.read.common.type.Type type) {
    return new UDFInputSeriesDataTypeNotValidException(
        0,
        UDFDataTypeTransformer.transformReadTypeToUDFDataType(type),
        Type.INT32,
        Type.INT64,
        Type.FLOAT,
        Type.DOUBLE);
  }

  // Keep each window algorithm's primitive output type while selecting its callback once.
  private static <T> TypeService<NumericWindowTransformer<T>> numericWindowTransformerService(
      NumericWindowTransformer<T> intTransformer,
      NumericWindowTransformer<T> longTransformer,
      NumericWindowTransformer<T> floatTransformer,
      NumericWindowTransformer<T> doubleTransformer) {
    return type ->
        switch (type.getTypeEnum()) {
          case INT32 -> intTransformer;
          case INT64 -> longTransformer;
          case FLOAT -> floatTransformer;
          case DOUBLE -> doubleTransformer;
          case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
              (target, rowWindow, collector) -> {
                throw invalidNumericDataType(type);
              };
        };
  }

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

  @FunctionalInterface
  interface NumericRowReader {
    double read(Row row) throws UDFInputSeriesDataTypeNotValidException, IOException;
  }

  @FunctionalInterface
  interface NumericColumnReader {
    double read(Column column, int position) throws UDFInputSeriesDataTypeNotValidException;
  }

  @FunctionalInterface
  interface NumericRowCollector {
    void collect(Row row, PointCollector collector)
        throws UDFInputSeriesDataTypeNotValidException, IOException;
  }

  @FunctionalInterface
  interface AbsRowCollector {
    void collect(Row row, PointCollector collector)
        throws UDFInputSeriesDataTypeNotValidException, IOException;
  }

  @FunctionalInterface
  interface AbsRowMapper {
    Object map(Row row) throws UDFInputSeriesDataTypeNotValidException, IOException;
  }

  @FunctionalInterface
  interface AbsColumnTransformer {
    void transform(UDTFAbs target, Column[] columns, ColumnBuilder builder)
        throws UDFInputSeriesDataTypeNotValidException;
  }

  @FunctionalInterface
  interface NumericWindowTransformer<T> {
    void transform(T target, RowWindow rowWindow, PointCollector collector)
        throws UDFInputSeriesDataTypeNotValidException, IOException;
  }

  @FunctionalInterface
  interface ChangePointProcessor {
    void transform(UDTFChangePoints target, Row row, PointCollector collector) throws IOException;
  }

  @FunctionalInterface
  interface SelectKRowTransformer {
    void transform(UDTFSelectK target, Row row)
        throws UDFInputSeriesDataTypeNotValidException, IOException;
  }

  @FunctionalInterface
  interface SelectKTerminator {
    void terminate(UDTFSelectK target, PointCollector collector)
        throws UDFInputSeriesDataTypeNotValidException, IOException;
  }

  @FunctionalInterface
  interface TopKQueueConstructor {
    void construct(UDTFTopK target);
  }

  @FunctionalInterface
  interface BottomKQueueConstructor {
    void construct(UDTFBottomK target);
  }

  @FunctionalInterface
  interface ConstantParser {
    void parse(UDTFConst target, UDFParameters parameters);
  }

  @FunctionalInterface
  interface ConstantRowCollector {
    void collect(UDTFConst target, Row row, PointCollector collector) throws IOException;
  }

  @FunctionalInterface
  interface ConstantRowMapper {
    Object map(UDTFConst target);
  }

  @FunctionalInterface
  interface ConstantColumnValueWriter {
    void write(UDTFConst target, ColumnBuilder builder);
  }

  @FunctionalInterface
  interface ContinuouslySatisfyRowTransformer {
    boolean transform(UDTFContinuouslySatisfy target, Row row)
        throws UDFInputSeriesDataTypeNotValidException, IOException;
  }

  @FunctionalInterface
  interface ContinuouslySatisfyTerminator {
    void terminate(UDTFContinuouslySatisfy target, PointCollector collector)
        throws UDFInputSeriesDataTypeNotValidException, IOException;
  }
}
