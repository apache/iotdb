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

package org.apache.iotdb.calc.utils;

import org.apache.iotdb.calc.execution.operator.process.fill.IFill;
import org.apache.iotdb.calc.execution.operator.process.fill.IFillFilter;
import org.apache.iotdb.calc.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.identity.IdentityLinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.linear.DoubleLinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.linear.FloatLinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.linear.IntLinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.linear.LongLinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.next.BinaryNextFill;
import org.apache.iotdb.calc.execution.operator.process.fill.next.BooleanNextFill;
import org.apache.iotdb.calc.execution.operator.process.fill.next.DoubleNextFill;
import org.apache.iotdb.calc.execution.operator.process.fill.next.FloatNextFill;
import org.apache.iotdb.calc.execution.operator.process.fill.next.IntNextFill;
import org.apache.iotdb.calc.execution.operator.process.fill.next.LongNextFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.BinaryPreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.BinaryPreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.BooleanPreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.BooleanPreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.DoublePreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.DoublePreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.FloatPreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.FloatPreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.IntPreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.IntPreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.LongPreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.LongPreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.AscBinaryTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.AscBooleanTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.AscDoubleTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.AscFloatTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.AscIntTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.AscLongTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.DescBinaryTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.DescBooleanTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.DescDoubleTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.DescFloatTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.DescIntTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.DescLongTypeJoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.JoinKeyComparator;
import org.apache.iotdb.calc.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.calc.execution.operator.process.window.utils.ColumnList;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.approximate.HyperLogLog;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedMaxMinByBaseAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.BinaryBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.BooleanBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.FloatBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.IntBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.BinaryGreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.BinaryLeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.BooleanGreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.BooleanLeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.DoubleGreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.DoubleLeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.FloatGreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.FloatLeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.Int32GreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.Int32LeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.Int64GreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.Int64LeastColumnTransformer;
import org.apache.iotdb.calc.utils.datastructure.SortKey;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

import static org.apache.iotdb.calc.transformation.datastructure.util.BinaryUtils.MIN_ARRAY_HEADER_SIZE;
import static org.apache.iotdb.calc.transformation.datastructure.util.BinaryUtils.MIN_OBJECT_HEADER_SIZE;

public class TypeServices {

  private static final IdentityLinearFill IDENTITY_LINEAR_FILL = new IdentityLinearFill();

  // Unsupported fill types defer their exception until the returned strategy is used so check()
  // can still validate that every TypeEnum has a service entry.
  public static final TypeService<Supplier<ILinearFill>> LINEAR_FILL_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> IntLinearFill::new;
            case INT64, TIMESTAMP -> LongLinearFill::new;
            case FLOAT -> FloatLinearFill::new;
            case DOUBLE -> DoubleLinearFill::new;
            case BOOLEAN, TEXT, STRING, BLOB, OBJECT -> () -> IDENTITY_LINEAR_FILL;
            case ROW, UNKNOWN, VECTOR ->
                () -> {
                  throw unsupportedDataType(type);
                };
          };

  public static final TypeService<Function<IFillFilter, IFill>> PREVIOUS_FILL_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                filter ->
                    filter == null
                        ? new BooleanPreviousFill()
                        : new BooleanPreviousFillWithTimeDuration(filter);
            case TEXT, STRING, BLOB, OBJECT ->
                filter ->
                    filter == null
                        ? new BinaryPreviousFill()
                        : new BinaryPreviousFillWithTimeDuration(filter);
            case INT32, DATE ->
                filter ->
                    filter == null
                        ? new IntPreviousFill()
                        : new IntPreviousFillWithTimeDuration(filter);
            case INT64, TIMESTAMP ->
                filter ->
                    filter == null
                        ? new LongPreviousFill()
                        : new LongPreviousFillWithTimeDuration(filter);
            case FLOAT ->
                filter ->
                    filter == null
                        ? new FloatPreviousFill()
                        : new FloatPreviousFillWithTimeDuration(filter);
            case DOUBLE ->
                filter ->
                    filter == null
                        ? new DoublePreviousFill()
                        : new DoublePreviousFillWithTimeDuration(filter);
            case ROW, UNKNOWN, VECTOR ->
                filter -> {
                  throw unsupportedDataType(type);
                };
          };

  public static final TypeService<Function<IFillFilter, ILinearFill>> NEXT_FILL_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> BooleanNextFill::new;
            case TEXT, STRING, BLOB, OBJECT -> BinaryNextFill::new;
            case INT32, DATE -> IntNextFill::new;
            case INT64, TIMESTAMP -> LongNextFill::new;
            case FLOAT -> FloatNextFill::new;
            case DOUBLE -> DoubleNextFill::new;
            case ROW, UNKNOWN, VECTOR ->
                filter -> {
                  throw unsupportedDataType(type);
                };
          };

  public static final TypeService<BooleanFunction<JoinKeyComparator>> JOIN_KEY_COMPARATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                ascending ->
                    ascending
                        ? AscIntTypeJoinKeyComparator.getInstance()
                        : DescIntTypeJoinKeyComparator.getInstance();
            case INT64, TIMESTAMP ->
                ascending ->
                    ascending
                        ? AscLongTypeJoinKeyComparator.getInstance()
                        : DescLongTypeJoinKeyComparator.getInstance();
            case FLOAT ->
                ascending ->
                    ascending
                        ? AscFloatTypeJoinKeyComparator.getInstance()
                        : DescFloatTypeJoinKeyComparator.getInstance();
            case DOUBLE ->
                ascending ->
                    ascending
                        ? AscDoubleTypeJoinKeyComparator.getInstance()
                        : DescDoubleTypeJoinKeyComparator.getInstance();
            case BOOLEAN ->
                ascending ->
                    ascending
                        ? AscBooleanTypeJoinKeyComparator.getInstance()
                        : DescBooleanTypeJoinKeyComparator.getInstance();
            case STRING, BLOB, TEXT ->
                ascending ->
                    ascending
                        ? AscBinaryTypeJoinKeyComparator.getInstance()
                        : DescBinaryTypeJoinKeyComparator.getInstance();
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                ascending -> {
                  throw new UnsupportedOperationException(
                      CalcMessages.UNSUPPORTED_DATA_TYPE + type);
                };
          };

  public static final TypeService<IntFunction<Comparator<SortKey>>> MERGE_SORT_COMPARATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                index ->
                    Comparator.comparingInt(
                        sortKey -> type.getInt(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
            case INT64, TIMESTAMP ->
                index ->
                    Comparator.comparingLong(
                        sortKey ->
                            type.getLong(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
            case FLOAT ->
                index ->
                    Comparator.comparingDouble(
                        sortKey ->
                            type.getFloat(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
            case DOUBLE ->
                index ->
                    Comparator.comparingDouble(
                        sortKey ->
                            type.getDouble(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
            case TEXT, STRING, BLOB, OBJECT ->
                index ->
                    Comparator.comparing(
                        sortKey ->
                            type.getBinary(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
            case BOOLEAN ->
                index ->
                    Comparator.comparing(
                        sortKey ->
                            type.getBoolean(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
            // TypeService.check() must be able to build a strategy for every enum value.
            case ROW, UNKNOWN, VECTOR ->
                index -> {
                  throw new IllegalArgumentException(
                      String.format(CalcMessages.DATA_TYPE_CANNOT_BE_ORDERED, type));
                };
          };

  public static final TypeService<Integer> MEMORY_USAGE_OF_ONE_MERGE_SORT_KEY_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> 1;
            case INT32, FLOAT, DATE -> 4;
            case INT64, DOUBLE, TIMESTAMP -> 8;
            case TEXT, STRING, BLOB, OBJECT -> 16;
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type)
                    .setChecked(true);
          };

  public static final TypeService<IntUnaryOperator>
      MEMORY_USAGE_OF_ONE_SERIALIZABLE_ROW_FIELD_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, DATE -> ignored -> ReadWriteIOUtils.INT_LEN;
                case INT64, TIMESTAMP -> ignored -> ReadWriteIOUtils.LONG_LEN;
                case FLOAT -> ignored -> ReadWriteIOUtils.FLOAT_LEN;
                case DOUBLE -> ignored -> ReadWriteIOUtils.DOUBLE_LEN;
                case BOOLEAN -> ignored -> ReadWriteIOUtils.BOOLEAN_LEN;
                case TEXT, BLOB, STRING, OBJECT ->
                    byteArrayLength ->
                        MIN_OBJECT_HEADER_SIZE + MIN_ARRAY_HEADER_SIZE + byteArrayLength;
                case ROW, UNKNOWN, VECTOR ->
                    throw new UnSupportedDataTypeException(type.toString()).setChecked(true);
              };

  public static final TypeService<Function<List<ColumnTransformer>, ColumnTransformer>>
      GREATEST_COLUMN_TRANSFORMER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN ->
                    columnTransformers ->
                        new BooleanGreatestColumnTransformer(type, columnTransformers);
                case INT32, DATE ->
                    columnTransformers ->
                        new Int32GreatestColumnTransformer(type, columnTransformers);
                case INT64, TIMESTAMP ->
                    columnTransformers ->
                        new Int64GreatestColumnTransformer(type, columnTransformers);
                case FLOAT ->
                    columnTransformers ->
                        new FloatGreatestColumnTransformer(type, columnTransformers);
                case DOUBLE ->
                    columnTransformers ->
                        new DoubleGreatestColumnTransformer(type, columnTransformers);
                case STRING, TEXT ->
                    columnTransformers ->
                        new BinaryGreatestColumnTransformer(type, columnTransformers);
                case BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                    columnTransformers -> {
                      throw unsupportedGreatestLeastDataType(type);
                    };
              };

  public static final TypeService<Function<List<ColumnTransformer>, ColumnTransformer>>
      LEAST_COLUMN_TRANSFORMER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN ->
                    columnTransformers ->
                        new BooleanLeastColumnTransformer(type, columnTransformers);
                case INT32, DATE ->
                    columnTransformers -> new Int32LeastColumnTransformer(type, columnTransformers);
                case INT64, TIMESTAMP ->
                    columnTransformers -> new Int64LeastColumnTransformer(type, columnTransformers);
                case FLOAT ->
                    columnTransformers -> new FloatLeastColumnTransformer(type, columnTransformers);
                case DOUBLE ->
                    columnTransformers ->
                        new DoubleLeastColumnTransformer(type, columnTransformers);
                case STRING, TEXT ->
                    columnTransformers ->
                        new BinaryLeastColumnTransformer(type, columnTransformers);
                case BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                    columnTransformers -> {
                      throw unsupportedGreatestLeastDataType(type);
                    };
              };

  /**
   * Creates a single-row column containing the value at the requested row. Delegating the write to
   * {@link Type} preserves type-specific column metadata (for example, DATE) without another
   * TsDataType dispatch.
   */
  public static final TypeService<ColumnRowFunction> UPDATE_LAST_ROW_SERVICE =
      type ->
          (column, rowIndex) -> {
            // Use the status overload so specialized types such as DATE retain their data type.
            ColumnBuilder columnBuilder = type.createColumnBuilder(null, 1);
            type.write(columnBuilder, column, rowIndex);
            return columnBuilder.build();
          };

  /** Copies a column value into a reusable primitive result without boxing. */
  public static final TypeService<PrimitiveColumnValueSetter>
      PRIMITIVE_COLUMN_VALUE_SETTER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, DATE ->
                    (target, column, position) -> target.setInt(type.getInt(column, position));
                case INT64, TIMESTAMP ->
                    (target, column, position) -> target.setLong(type.getLong(column, position));
                case FLOAT ->
                    (target, column, position) -> target.setFloat(type.getFloat(column, position));
                case DOUBLE ->
                    (target, column, position) ->
                        target.setDouble(type.getDouble(column, position));
                case BOOLEAN ->
                    (target, column, position) ->
                        target.setBoolean(type.getBoolean(column, position));
                case TEXT, STRING, BLOB, OBJECT ->
                    (target, column, position) ->
                        target.setBinary(type.getBinary(column, position));
                case ROW, UNKNOWN, VECTOR ->
                    (target, column, position) -> {
                      throw new UnSupportedDataTypeException(
                              CalcMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum())
                          .setChecked(true);
                    };
              };

  /** Selects the y-column reader used by MAX_BY and MIN_BY input processing. */
  public static final TypeService<MaxMinByInput> MAX_MIN_BY_INPUT_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateInt(
                                arguments[1].getInt(position), arguments[0], position));
            case INT64, TIMESTAMP ->
                (arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateLong(
                                arguments[1].getLong(position), arguments[0], position));
            case FLOAT ->
                (arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateFloat(
                                arguments[1].getFloat(position), arguments[0], position));
            case DOUBLE ->
                (arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateDouble(
                                arguments[1].getDouble(position), arguments[0], position));
            case TEXT, STRING, BLOB, OBJECT ->
                (arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateBinary(
                                arguments[1].getBinary(position), arguments[0], position));
            case BOOLEAN ->
                (arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateBoolean(
                                arguments[1].getBoolean(position), arguments[0], position));
            case ROW, UNKNOWN, VECTOR ->
                (arguments, mask, updater) -> {
                  throw unsupportedMaxMinByDataType(type);
                };
          };

  /** Selects the y-value decoder used by MAX_BY and MIN_BY intermediate input processing. */
  public static final TypeService<MaxMinByIntermediateInput> MAX_MIN_BY_INTERMEDIATE_INPUT_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (bytes, xType, columnBuilder, updater) -> {
                  int value = BytesUtils.bytesToInt(bytes, 0);
                  readMaxMinByXFromBytes(bytes, Integer.BYTES, xType, columnBuilder);
                  updater.updateInt(value, columnBuilder.build(), 0);
                };
            case INT64, TIMESTAMP ->
                (bytes, xType, columnBuilder, updater) -> {
                  long value = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
                  readMaxMinByXFromBytes(bytes, Long.BYTES, xType, columnBuilder);
                  updater.updateLong(value, columnBuilder.build(), 0);
                };
            case FLOAT ->
                (bytes, xType, columnBuilder, updater) -> {
                  float value = BytesUtils.bytesToFloat(bytes, 0);
                  readMaxMinByXFromBytes(bytes, Float.BYTES, xType, columnBuilder);
                  updater.updateFloat(value, columnBuilder.build(), 0);
                };
            case DOUBLE ->
                (bytes, xType, columnBuilder, updater) -> {
                  double value = BytesUtils.bytesToDouble(bytes, 0);
                  readMaxMinByXFromBytes(bytes, Double.BYTES, xType, columnBuilder);
                  updater.updateDouble(value, columnBuilder.build(), 0);
                };
            case TEXT, STRING, BLOB, OBJECT ->
                (bytes, xType, columnBuilder, updater) -> {
                  int length = BytesUtils.bytesToInt(bytes, 0);
                  int xOffset = Integer.BYTES + length;
                  Binary value = new Binary(BytesUtils.subBytes(bytes, Integer.BYTES, length));
                  readMaxMinByXFromBytes(bytes, xOffset, xType, columnBuilder);
                  updater.updateBinary(value, columnBuilder.build(), 0);
                };
            case BOOLEAN ->
                (bytes, xType, columnBuilder, updater) -> {
                  boolean value = BytesUtils.bytesToBool(bytes, 0);
                  readMaxMinByXFromBytes(bytes, 1, xType, columnBuilder);
                  updater.updateBoolean(value, columnBuilder.build(), 0);
                };
            case ROW, UNKNOWN, VECTOR ->
                (bytes, xType, columnBuilder, updater) -> {
                  throw unsupportedMaxMinByDataType(type);
                };
          };

  /** Selects the type-specific decoder for a LAST intermediate value. */
  public static final TypeService<LastValueDeserializer> LAST_VALUE_DESERIALIZER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (bytes, offset, time, isOrderTimeNull, updater) -> {
                  int value = BytesUtils.bytesToInt(bytes, offset);
                  updater.updateInt(value, time, isOrderTimeNull);
                };
            case INT64, TIMESTAMP ->
                (bytes, offset, time, isOrderTimeNull, updater) -> {
                  long value = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
                  updater.updateLong(value, time, isOrderTimeNull);
                };
            case FLOAT ->
                (bytes, offset, time, isOrderTimeNull, updater) -> {
                  float value = BytesUtils.bytesToFloat(bytes, offset);
                  updater.updateFloat(value, time, isOrderTimeNull);
                };
            case DOUBLE ->
                (bytes, offset, time, isOrderTimeNull, updater) -> {
                  double value = BytesUtils.bytesToDouble(bytes, offset);
                  updater.updateDouble(value, time, isOrderTimeNull);
                };
            case TEXT, BLOB, OBJECT, STRING ->
                (bytes, offset, time, isOrderTimeNull, updater) -> {
                  int length = BytesUtils.bytesToInt(bytes, offset);
                  Binary value =
                      new Binary(BytesUtils.subBytes(bytes, offset + Integer.BYTES, length));
                  updater.updateBinary(value, time, isOrderTimeNull);
                };
            case BOOLEAN ->
                (bytes, offset, time, isOrderTimeNull, updater) ->
                    updater.updateBoolean(
                        BytesUtils.bytesToBool(bytes, offset), time, isOrderTimeNull);
            case ROW, UNKNOWN, VECTOR ->
                (bytes, offset, time, isOrderTimeNull, updater) -> {
                  throw new UnSupportedDataTypeException(
                      CalcMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  public static final TypeService<Function<TsPrimitiveType, Object>>
      PRIMITIVE_TYPE_VALUE_EXTRACTOR_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN, INT32, INT64, TIMESTAMP, FLOAT, DOUBLE -> TsPrimitiveType::getValue;
                case DATE -> primitiveType -> DateUtils.parseIntToLocalDate(primitiveType.getInt());
                case TEXT, BLOB, STRING ->
                    primitiveType -> {
                      final Binary binary = primitiveType.getBinary();
                      return binary == null || binary.getValues() == null
                          ? Binary.EMPTY_VALUE
                          : binary;
                    };
                case OBJECT, ROW, UNKNOWN, VECTOR ->
                    primitiveType -> {
                      throw new UnSupportedDataTypeException(
                              CalcMessages.UNSUPPORTED_DATA_TYPE + primitiveType.getDataType())
                          .setChecked(true);
                    };
              };

  /** Reads, serializes, deserializes, and writes Mode values without TsPrimitiveType conversion. */
  public static final TypeService<ModeValueService> MODE_VALUE_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                modeValueService(
                    type,
                    type::getBoolean,
                    BytesUtils::bytesToBool,
                    (value, bytes, offset) -> {
                      BytesUtils.boolToBytes((boolean) value, bytes, offset);
                      return Byte.BYTES;
                    },
                    (columnBuilder, value) -> type.writeBoolean(columnBuilder, (boolean) value));
            case INT32, DATE ->
                modeValueService(
                    type,
                    type::getInt,
                    BytesUtils::bytesToInt,
                    (value, bytes, offset) -> {
                      BytesUtils.intToBytes((int) value, bytes, offset);
                      return Integer.BYTES;
                    },
                    (columnBuilder, value) -> type.writeInt(columnBuilder, (int) value));
            case INT64, TIMESTAMP ->
                modeValueService(
                    type,
                    type::getLong,
                    (bytes, offset) -> BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset),
                    (value, bytes, offset) -> {
                      BytesUtils.longToBytes((long) value, bytes, offset);
                      return Long.BYTES;
                    },
                    (columnBuilder, value) -> type.writeLong(columnBuilder, (long) value));
            case FLOAT ->
                modeValueService(
                    type,
                    type::getFloat,
                    BytesUtils::bytesToFloat,
                    (value, bytes, offset) -> {
                      BytesUtils.floatToBytes((float) value, bytes, offset);
                      return Float.BYTES;
                    },
                    (columnBuilder, value) -> type.writeFloat(columnBuilder, (float) value));
            case DOUBLE ->
                modeValueService(
                    type,
                    type::getDouble,
                    BytesUtils::bytesToDouble,
                    (value, bytes, offset) -> {
                      BytesUtils.doubleToBytes((double) value, bytes, offset);
                      return Double.BYTES;
                    },
                    (columnBuilder, value) -> type.writeDouble(columnBuilder, (double) value));
            case TEXT, STRING, BLOB ->
                modeValueService(
                    type,
                    type::getBinary,
                    TypeServices::deserializeModeBinary,
                    TypeServices::serializeModeBinary,
                    (columnBuilder, value) -> type.writeBinary(columnBuilder, (Binary) value));
            case OBJECT, ROW, UNKNOWN, VECTOR -> unsupportedModeValueService(type);
          };

  // The caller supplies the exception so shared conversion keeps each aggregation API's contract.
  public static final TypeService<ColumnToDoubleConverterFactory>
      NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, DATE, INT64, TIMESTAMP, FLOAT, DOUBLE -> ignored -> type::getDouble;
                case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                    exceptionSupplier ->
                        (column, position) -> {
                          throw exceptionSupplier.get();
                        };
              };

  /** Numeric inputs supported by SUM, AVG and percentile aggregations. */
  public static final TypeService<ColumnToDoubleConverterFactory>
      AGGREGATION_NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, INT64, FLOAT, DOUBLE -> ignored -> type::getDouble;
                case BOOLEAN, DATE, TIMESTAMP, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                    exceptionSupplier ->
                        (column, position) -> {
                          throw exceptionSupplier.get();
                        };
              };

  /** Numeric inputs supported by percentile aggregations, including TIMESTAMP. */
  public static final TypeService<ColumnToDoubleConverterFactory>
      PERCENTILE_NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, INT64, TIMESTAMP, FLOAT, DOUBLE -> ignored -> type::getDouble;
                case BOOLEAN, DATE, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                    exceptionSupplier ->
                        (column, position) -> {
                          throw exceptionSupplier.get();
                        };
              };

  /** Writes a numeric percentile result in the input column's native type. */
  public static final TypeService<NumericResultWriter> NUMERIC_RESULT_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> (builder, value) -> type.writeInt(builder, (int) value);
            case INT64, TIMESTAMP -> (builder, value) -> type.writeLong(builder, (long) value);
            case FLOAT -> (builder, value) -> type.writeFloat(builder, (float) value);
            case DOUBLE -> (builder, value) -> type.writeDouble(builder, value);
            case BOOLEAN, DATE, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                (builder, value) -> {
                  throw new UnSupportedDataTypeException(
                      CalcMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  /** Updates a reusable primitive result with a column value when it is more extreme. */
  public static final TypeService<ColumnValueUpdater> MIN_COLUMN_VALUE_UPDATER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (result, column, position, initialized) -> {
                  int value = type.getInt(column, position);
                  if (!initialized || value < result.getInt()) {
                    result.setInt(value);
                    return true;
                  }
                  return false;
                };
            case INT64, TIMESTAMP ->
                (result, column, position, initialized) -> {
                  long value = type.getLong(column, position);
                  if (!initialized || value < result.getLong()) {
                    result.setLong(value);
                    return true;
                  }
                  return false;
                };
            case FLOAT ->
                (result, column, position, initialized) -> {
                  float value = type.getFloat(column, position);
                  if (!initialized || value < result.getFloat()) {
                    result.setFloat(value);
                    return true;
                  }
                  return false;
                };
            case DOUBLE ->
                (result, column, position, initialized) -> {
                  double value = type.getDouble(column, position);
                  if (!initialized || value < result.getDouble()) {
                    result.setDouble(value);
                    return true;
                  }
                  return false;
                };
            case BOOLEAN ->
                (result, column, position, initialized) -> {
                  boolean value = type.getBoolean(column, position);
                  if (!initialized || !value) {
                    result.setBoolean(value);
                    return true;
                  }
                  return false;
                };
            case TEXT, STRING, BLOB ->
                (result, column, position, initialized) -> {
                  Binary value = type.getBinary(column, position);
                  if (!initialized || value.compareTo(result.getBinary()) < 0) {
                    result.setBinary(value);
                    return true;
                  }
                  return false;
                };
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (result, column, position, initialized) -> {
                  throw new UnSupportedDataTypeException(
                      CalcMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  /** Updates a reusable primitive result with a column value when it is more extreme. */
  public static final TypeService<ColumnValueUpdater> MAX_COLUMN_VALUE_UPDATER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (result, column, position, initialized) -> {
                  int value = type.getInt(column, position);
                  if (!initialized || value > result.getInt()) {
                    result.setInt(value);
                    return true;
                  }
                  return false;
                };
            case INT64, TIMESTAMP ->
                (result, column, position, initialized) -> {
                  long value = type.getLong(column, position);
                  if (!initialized || value > result.getLong()) {
                    result.setLong(value);
                    return true;
                  }
                  return false;
                };
            case FLOAT ->
                (result, column, position, initialized) -> {
                  float value = type.getFloat(column, position);
                  if (!initialized || value > result.getFloat()) {
                    result.setFloat(value);
                    return true;
                  }
                  return false;
                };
            case DOUBLE ->
                (result, column, position, initialized) -> {
                  double value = type.getDouble(column, position);
                  if (!initialized || value > result.getDouble()) {
                    result.setDouble(value);
                    return true;
                  }
                  return false;
                };
            case BOOLEAN ->
                (result, column, position, initialized) -> {
                  boolean value = type.getBoolean(column, position);
                  if (!initialized || value) {
                    result.setBoolean(value);
                    return true;
                  }
                  return false;
                };
            case TEXT, STRING, BLOB ->
                (result, column, position, initialized) -> {
                  Binary value = type.getBinary(column, position);
                  if (!initialized || value.compareTo(result.getBinary()) > 0) {
                    result.setBinary(value);
                    return true;
                  }
                  return false;
                };
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (result, column, position, initialized) -> {
                  throw new UnSupportedDataTypeException(
                      CalcMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  /** Updates a reusable primitive result with the value having the greatest absolute magnitude. */
  public static final TypeService<ColumnValueUpdater> EXTREME_COLUMN_VALUE_UPDATER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (result, column, position, initialized) -> {
                  int value = type.getInt(column, position);
                  int candidate = result.getInt();
                  if (!initialized || compareExtreme(value, candidate) > 0) {
                    result.setInt(value);
                    return true;
                  }
                  return false;
                };
            case INT64 ->
                (result, column, position, initialized) -> {
                  long value = type.getLong(column, position);
                  long candidate = result.getLong();
                  if (!initialized || compareExtreme(value, candidate) > 0) {
                    result.setLong(value);
                    return true;
                  }
                  return false;
                };
            case FLOAT ->
                (result, column, position, initialized) -> {
                  float value = type.getFloat(column, position);
                  float candidate = result.getFloat();
                  float absValue = Math.abs(value);
                  float absCandidate = Math.abs(candidate);
                  if (!initialized
                      || absValue > absCandidate
                      || absValue == absCandidate && value > candidate) {
                    result.setFloat(value);
                    return true;
                  }
                  return false;
                };
            case DOUBLE ->
                (result, column, position, initialized) -> {
                  double value = type.getDouble(column, position);
                  double candidate = result.getDouble();
                  double absValue = Math.abs(value);
                  double absCandidate = Math.abs(candidate);
                  if (!initialized
                      || absValue > absCandidate
                      || absValue == absCandidate && value > candidate) {
                    result.setDouble(value);
                    return true;
                  }
                  return false;
                };
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (result, column, position, initialized) -> {
                  throw new UnSupportedDataTypeException(
                      CalcMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  /** Adds a column value to an HLL using the column's native primitive accessor. */
  public static final TypeService<HyperLogLogColumnAdder> HYPER_LOG_LOG_COLUMN_ADDER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> (hll, column, position) -> hll.add(column.getBoolean(position));
            case INT32, DATE -> (hll, column, position) -> hll.add(column.getInt(position));
            case INT64, TIMESTAMP -> (hll, column, position) -> hll.add(column.getLong(position));
            case FLOAT -> (hll, column, position) -> hll.add(column.getFloat(position));
            case DOUBLE -> (hll, column, position) -> hll.add(column.getDouble(position));
            case TEXT, BLOB, STRING, OBJECT ->
                (hll, column, position) -> hll.add(column.getBinary(position));
            case ROW, UNKNOWN, VECTOR ->
                (hll, column, position) -> {
                  throw new UnSupportedDataTypeException(
                          CalcMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum())
                      .setChecked(true);
                };
          };

  // RANGE frame offsets must retain each primitive type's native overflow and precision rules.
  public static final TypeService<RangeFrameComparator> RANGE_FRAME_COMPARATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (column,
                    partition,
                    currentIndex,
                    recentIndex,
                    channel,
                    offsetOperation,
                    comparison) -> {
                  int current = column.getInt(currentIndex);
                  int offset = partition.getInt(channel, currentIndex);
                  int boundary = offsetOperation.apply(current, offset);
                  return comparison.compare(column.getInt(recentIndex), boundary);
                };
            case INT64, TIMESTAMP ->
                (column,
                    partition,
                    currentIndex,
                    recentIndex,
                    channel,
                    offsetOperation,
                    comparison) -> {
                  long current = column.getLong(currentIndex);
                  long offset = partition.getLong(channel, currentIndex);
                  long boundary = offsetOperation.apply(current, offset);
                  return comparison.compare(column.getLong(recentIndex), boundary);
                };
            case FLOAT ->
                (column,
                    partition,
                    currentIndex,
                    recentIndex,
                    channel,
                    offsetOperation,
                    comparison) -> {
                  float current = column.getFloat(currentIndex);
                  float offset = partition.getFloat(channel, currentIndex);
                  float boundary = offsetOperation.apply(current, offset);
                  return comparison.compare(column.getFloat(recentIndex), boundary);
                };
            case DOUBLE ->
                (column,
                    partition,
                    currentIndex,
                    recentIndex,
                    channel,
                    offsetOperation,
                    comparison) -> {
                  double current = column.getDouble(currentIndex);
                  double offset = partition.getDouble(channel, currentIndex);
                  double boundary = offsetOperation.apply(current, offset);
                  return comparison.compare(column.getDouble(recentIndex), boundary);
                };
            case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                (column,
                    partition,
                    currentIndex,
                    recentIndex,
                    channel,
                    offsetOperation,
                    comparison) -> {
                  throw new UnSupportedDataTypeException(CalcMessages.UNSUPPORTED_DATA_TYPE + type);
                };
          };

  public static final TypeService<Function<DefaultEncodingProvider, TSEncoding>>
      DEFAULT_ENCODING_BY_TYPE_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN -> DefaultEncodingProvider::getDefaultBooleanEncoding;
                case INT32, DATE -> DefaultEncodingProvider::getDefaultInt32Encoding;
                case INT64, TIMESTAMP -> DefaultEncodingProvider::getDefaultInt64Encoding;
                case FLOAT -> DefaultEncodingProvider::getDefaultFloatEncoding;
                case DOUBLE -> DefaultEncodingProvider::getDefaultDoubleEncoding;
                case STRING, BLOB, OBJECT, TEXT -> DefaultEncodingProvider::getDefaultTextEncoding;
                case ROW, UNKNOWN, VECTOR ->
                    throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type)
                        .setChecked(true);
              };

  public static final TypeService<DefaultValueWriter> DEFAULT_VALUE_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (partition, channel, index, builder) ->
                    builder.writeInt(partition.getInt(channel, index));
            case INT64, TIMESTAMP ->
                (partition, channel, index, builder) ->
                    builder.writeLong(partition.getLong(channel, index));
            case FLOAT ->
                (partition, channel, index, builder) ->
                    builder.writeFloat(partition.getFloat(channel, index));
            case DOUBLE ->
                (partition, channel, index, builder) ->
                    builder.writeDouble(partition.getDouble(channel, index));
            case BOOLEAN ->
                (partition, channel, index, builder) ->
                    builder.writeBoolean(partition.getBoolean(channel, index));
            case TEXT, STRING, BLOB, OBJECT ->
                (partition, channel, index, builder) ->
                    builder.writeBinary(partition.getBinary(channel, index));
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        CalcMessages.UNSUPPORTED_DEFAULT_VALUE_DATA_TYPE_IN_LAG + type)
                    .setChecked(true);
          };

  public static final TypeService<IntermediateValueWriter> INTERMEDIATE_VALUE_WRITER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (accumulator, isX, index, bytes, offset) ->
                    (isX ? accumulator.getXIntValues() : accumulator.getYIntValues())
                        .toBytes(index, bytes, offset);
            case INT64, TIMESTAMP ->
                (accumulator, isX, index, bytes, offset) ->
                    (isX ? accumulator.getXLongValues() : accumulator.getYLongValues())
                        .toBytes(index, bytes, offset);
            case FLOAT ->
                (accumulator, isX, index, bytes, offset) ->
                    (isX ? accumulator.getXFloatValues() : accumulator.getYFloatValues())
                        .toBytes(index, bytes, offset);
            case DOUBLE ->
                (accumulator, isX, index, bytes, offset) ->
                    (isX ? accumulator.getXDoubleValues() : accumulator.getYDoubleValues())
                        .toBytes(index, bytes, offset);
            case TEXT, STRING, BLOB, OBJECT ->
                (accumulator, isX, index, bytes, offset) ->
                    (isX ? accumulator.getXBinaryValues() : accumulator.getYBinaryValues())
                        .toBytes(index, bytes, offset);
            case BOOLEAN ->
                (accumulator, isX, index, bytes, offset) ->
                    (isX ? accumulator.getXBooleanValues() : accumulator.getYBooleanValues())
                        .toBytes(index, bytes, offset);
            case ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type)
                    .setChecked(true);
          };

  public static final TypeService<IntermediateValueInitializer>
      INTERMEDIATE_VALUE_INITIALIZER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, DATE ->
                    (accumulator, isX) -> {
                      if (isX) {
                        accumulator.setXIntValues(new IntBigArray());
                      } else {
                        accumulator.setYIntValues(new IntBigArray());
                      }
                    };
                case INT64, TIMESTAMP ->
                    (accumulator, isX) -> {
                      if (isX) {
                        accumulator.setXLongValues(new LongBigArray());
                      } else {
                        accumulator.setYLongValues(new LongBigArray());
                      }
                    };
                case FLOAT ->
                    (accumulator, isX) -> {
                      if (isX) {
                        accumulator.setXFloatValues(new FloatBigArray());
                      } else {
                        accumulator.setYFloatValues(new FloatBigArray());
                      }
                    };
                case DOUBLE ->
                    (accumulator, isX) -> {
                      if (isX) {
                        accumulator.setXDoubleValues(new DoubleBigArray());
                      } else {
                        accumulator.setYDoubleValues(new DoubleBigArray());
                      }
                    };
                case TEXT, STRING, BLOB, OBJECT ->
                    (accumulator, isX) -> {
                      if (isX) {
                        accumulator.setXBinaryValues(new BinaryBigArray());
                      } else {
                        accumulator.setYBinaryValues(new BinaryBigArray());
                      }
                    };
                case BOOLEAN ->
                    (accumulator, isX) -> {
                      if (isX) {
                        accumulator.setXBooleanValues(new BooleanBigArray());
                      } else {
                        accumulator.setYBooleanValues(new BooleanBigArray());
                      }
                    };
                case ROW, UNKNOWN, VECTOR ->
                    throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type)
                        .setChecked(true);
              };

  static {
    LINEAR_FILL_SERVICE.check();
    PREVIOUS_FILL_SERVICE.check();
    NEXT_FILL_SERVICE.check();
    JOIN_KEY_COMPARATOR_SERVICE.check();
    GREATEST_COLUMN_TRANSFORMER_SERVICE.check();
    LEAST_COLUMN_TRANSFORMER_SERVICE.check();
    UPDATE_LAST_ROW_SERVICE.check();
    PRIMITIVE_COLUMN_VALUE_SETTER_SERVICE.check();
    MAX_MIN_BY_INPUT_SERVICE.check();
    MAX_MIN_BY_INTERMEDIATE_INPUT_SERVICE.check();
    LAST_VALUE_DESERIALIZER_SERVICE.check();
    MERGE_SORT_COMPARATOR_SERVICE.check();
    MEMORY_USAGE_OF_ONE_MERGE_SORT_KEY_SERVICE.check();
    MEMORY_USAGE_OF_ONE_SERIALIZABLE_ROW_FIELD_SERVICE.check();
    PRIMITIVE_TYPE_VALUE_EXTRACTOR_SERVICE.check();
    MODE_VALUE_SERVICE.check();
    NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE.check();
    AGGREGATION_NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE.check();
    PERCENTILE_NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE.check();
    NUMERIC_RESULT_WRITER_SERVICE.check();
    MIN_COLUMN_VALUE_UPDATER_SERVICE.check();
    MAX_COLUMN_VALUE_UPDATER_SERVICE.check();
    EXTREME_COLUMN_VALUE_UPDATER_SERVICE.check();
    HYPER_LOG_LOG_COLUMN_ADDER_SERVICE.check();
    RANGE_FRAME_COMPARATOR_SERVICE.check();
    DEFAULT_ENCODING_BY_TYPE_SERVICE.check();
    DEFAULT_VALUE_WRITER_SERVICE.check();
    INTERMEDIATE_VALUE_WRITER_SERVICE.check();
    INTERMEDIATE_VALUE_INITIALIZER_SERVICE.check();
  }

  private TypeServices() {
    // util class doesn't need constructor
  }

  private static IllegalArgumentException unsupportedDataType(final Type type) {
    return new IllegalArgumentException(CalcMessages.UNKNOWN_DATATYPE + type.getTypeEnum());
  }

  private static UnsupportedOperationException unsupportedGreatestLeastDataType(final Type type) {
    return new UnsupportedOperationException(
        CalcMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
  }

  private static UnSupportedDataTypeException unsupportedMaxMinByDataType(final Type type) {
    return new UnSupportedDataTypeException(
        String.format(
            CalcMessages.UNSUPPORTED_DATA_TYPE_IN_MAX_BY_MIN_BY_AGGREGATION, type.getTypeEnum()));
  }

  private static ModeValueService modeValueService(
      Type type,
      ModeValueGetter getter,
      ModeValueDeserializer deserializer,
      ModeValueSerializer serializer,
      ModeValueWriter writer) {
    return new ModeValueService() {
      @Override
      public Object getValue(Column column, int position) {
        return getter.get(column, position);
      }

      @Override
      public Object deserialize(byte[] bytes, int offset) {
        return deserializer.deserialize(bytes, offset);
      }

      @Override
      public int calcTypeSize(Object value) {
        return type.calcTypeSize(value);
      }

      @Override
      public int serialize(Object value, byte[] bytes, int offset) {
        return serializer.serialize(value, bytes, offset);
      }

      @Override
      public void write(ColumnBuilder columnBuilder, Object value) {
        writer.write(columnBuilder, value);
      }
    };
  }

  private static ModeValueService unsupportedModeValueService(final Type type) {
    return new ModeValueService() {
      @Override
      public Object getValue(Column column, int position) {
        throw unsupportedModeValueDataType(type);
      }

      @Override
      public Object deserialize(byte[] bytes, int offset) {
        throw unsupportedModeValueDataType(type);
      }

      @Override
      public int calcTypeSize(Object value) {
        throw unsupportedModeValueDataType(type);
      }

      @Override
      public int serialize(Object value, byte[] bytes, int offset) {
        throw unsupportedModeValueDataType(type);
      }

      @Override
      public void write(ColumnBuilder columnBuilder, Object value) {
        throw unsupportedModeValueDataType(type);
      }
    };
  }

  private static UnSupportedDataTypeException unsupportedModeValueDataType(final Type type) {
    return new UnSupportedDataTypeException(CalcMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum())
        .setChecked(true);
  }

  private static int serializeModeBinary(Object value, byte[] bytes, int offset) {
    Binary binary = (Binary) value;
    BytesUtils.intToBytes(binary.getLength(), bytes, offset);
    System.arraycopy(binary.getValues(), 0, bytes, offset + Integer.BYTES, binary.getLength());
    return Integer.BYTES + binary.getLength();
  }

  private static Binary deserializeModeBinary(byte[] bytes, int offset) {
    int length = BytesUtils.bytesToInt(bytes, offset);
    return new Binary(BytesUtils.subBytes(bytes, offset + Integer.BYTES, length));
  }

  private static void forEachSelectedPosition(
      Column column, AggregationMask mask, IntConsumer positionConsumer) {
    int positionCount = mask.getSelectedPositionCount();
    if (mask.isSelectAll()) {
      for (int position = 0; position < positionCount; position++) {
        if (!column.isNull(position)) {
          positionConsumer.accept(position);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int position = selectedPositions[i];
        if (!column.isNull(position)) {
          positionConsumer.accept(position);
        }
      }
    }
  }

  private static void readMaxMinByXFromBytes(
      byte[] bytes, int offset, Type xType, ColumnBuilder columnBuilder) {
    if (BytesUtils.bytesToBool(bytes, offset)) {
      columnBuilder.appendNull();
    } else {
      xType.write(columnBuilder, bytes, offset + 1);
    }
  }

  private static int compareExtreme(int left, int right) {
    int absComparison = Long.compare(Math.abs((long) left), Math.abs((long) right));
    return absComparison == 0 ? Integer.compare(left, right) : absComparison;
  }

  private static int compareExtreme(long left, long right) {
    int absComparison = compareAbs(left, right);
    return absComparison == 0 ? Long.compare(left, right) : absComparison;
  }

  private static int compareAbs(long left, long right) {
    if (left == Long.MIN_VALUE) {
      return right == Long.MIN_VALUE ? 0 : 1;
    }
    if (right == Long.MIN_VALUE) {
      return -1;
    }
    return Long.compare(Math.abs(left), Math.abs(right));
  }

  public interface DefaultEncodingProvider {
    TSEncoding getDefaultBooleanEncoding();

    TSEncoding getDefaultInt32Encoding();

    TSEncoding getDefaultInt64Encoding();

    TSEncoding getDefaultFloatEncoding();

    TSEncoding getDefaultDoubleEncoding();

    TSEncoding getDefaultTextEncoding();
  }

  @FunctionalInterface
  public interface ColumnRowFunction {
    Column apply(Column column, int rowIndex);
  }

  @FunctionalInterface
  public interface PrimitiveColumnValueSetter {
    void set(TsPrimitiveType target, Column column, int position);
  }

  public interface ModeValueService {
    Object getValue(Column column, int position);

    Object deserialize(byte[] bytes, int offset);

    int calcTypeSize(Object value);

    int serialize(Object value, byte[] bytes, int offset);

    void write(ColumnBuilder columnBuilder, Object value);
  }

  @FunctionalInterface
  private interface ModeValueGetter {
    Object get(Column column, int position);
  }

  @FunctionalInterface
  private interface ModeValueDeserializer {
    Object deserialize(byte[] bytes, int offset);
  }

  @FunctionalInterface
  private interface ModeValueSerializer {
    int serialize(Object value, byte[] bytes, int offset);
  }

  @FunctionalInterface
  private interface ModeValueWriter {
    void write(ColumnBuilder columnBuilder, Object value);
  }

  public interface MaxMinByValueUpdater {
    void updateInt(int value, Column xColumn, int xIndex);

    void updateLong(long value, Column xColumn, int xIndex);

    void updateFloat(float value, Column xColumn, int xIndex);

    void updateDouble(double value, Column xColumn, int xIndex);

    void updateBinary(Binary value, Column xColumn, int xIndex);

    void updateBoolean(boolean value, Column xColumn, int xIndex);
  }

  @FunctionalInterface
  public interface MaxMinByInput {
    void add(Column[] arguments, AggregationMask mask, MaxMinByValueUpdater updater);
  }

  @FunctionalInterface
  public interface MaxMinByIntermediateInput {
    void update(
        byte[] bytes, Type xType, ColumnBuilder columnBuilder, MaxMinByValueUpdater updater);
  }

  @FunctionalInterface
  public interface LastValueDeserializer {
    void deserialize(
        byte[] bytes, int offset, long time, boolean isOrderTimeNull, LastValueUpdater updater);
  }

  public interface LastValueUpdater {
    void updateInt(int value, long time, boolean isOrderTimeNull);

    void updateLong(long value, long time, boolean isOrderTimeNull);

    void updateFloat(float value, long time, boolean isOrderTimeNull);

    void updateDouble(double value, long time, boolean isOrderTimeNull);

    void updateBinary(Binary value, long time, boolean isOrderTimeNull);

    void updateBoolean(boolean value, long time, boolean isOrderTimeNull);
  }

  @FunctionalInterface
  public interface ColumnToDoubleConverter {
    double convert(Column column, int position);
  }

  @FunctionalInterface
  public interface BooleanFunction<T> {
    // Keep primitive dispatchers unboxed when selecting ascending/descending strategies.
    T apply(boolean value);
  }

  @FunctionalInterface
  public interface ColumnToDoubleConverterFactory {
    ColumnToDoubleConverter create(Supplier<? extends RuntimeException> exceptionSupplier);
  }

  @FunctionalInterface
  public interface NumericResultWriter {
    void write(ColumnBuilder builder, double value);
  }

  @FunctionalInterface
  public interface ColumnValueUpdater {
    boolean update(TsPrimitiveType result, Column column, int position, boolean initialized);
  }

  @FunctionalInterface
  public interface HyperLogLogColumnAdder {
    void add(HyperLogLog hll, Column column, int position);
  }

  @FunctionalInterface
  public interface RangeFrameComparator {
    boolean compare(
        ColumnList column,
        Partition partition,
        int currentIndex,
        int recentIndex,
        int channel,
        RangeFrameOffsetOperation offsetOperation,
        RangeFrameComparison comparison);
  }

  public enum RangeFrameOffsetOperation {
    ADD {
      @Override
      int apply(int value, int offset) {
        return value + offset;
      }

      @Override
      long apply(long value, long offset) {
        return value + offset;
      }

      @Override
      float apply(float value, float offset) {
        return value + offset;
      }

      @Override
      double apply(double value, double offset) {
        return value + offset;
      }
    },
    SUBTRACT {
      @Override
      int apply(int value, int offset) {
        return value - offset;
      }

      @Override
      long apply(long value, long offset) {
        return value - offset;
      }

      @Override
      float apply(float value, float offset) {
        return value - offset;
      }

      @Override
      double apply(double value, double offset) {
        return value - offset;
      }
    };

    abstract int apply(int value, int offset);

    abstract long apply(long value, long offset);

    abstract float apply(float value, float offset);

    abstract double apply(double value, double offset);
  }

  public enum RangeFrameComparison {
    GREATER_THAN_OR_EQUAL {
      @Override
      boolean compare(int left, int right) {
        return left >= right;
      }

      @Override
      boolean compare(long left, long right) {
        return left >= right;
      }

      @Override
      boolean compare(float left, float right) {
        return left >= right;
      }

      @Override
      boolean compare(double left, double right) {
        return left >= right;
      }
    },
    GREATER_THAN {
      @Override
      boolean compare(int left, int right) {
        return left > right;
      }

      @Override
      boolean compare(long left, long right) {
        return left > right;
      }

      @Override
      boolean compare(float left, float right) {
        return left > right;
      }

      @Override
      boolean compare(double left, double right) {
        return left > right;
      }
    },
    LESS_THAN_OR_EQUAL {
      @Override
      boolean compare(int left, int right) {
        return left <= right;
      }

      @Override
      boolean compare(long left, long right) {
        return left <= right;
      }

      @Override
      boolean compare(float left, float right) {
        return left <= right;
      }

      @Override
      boolean compare(double left, double right) {
        return left <= right;
      }
    },
    LESS_THAN {
      @Override
      boolean compare(int left, int right) {
        return left < right;
      }

      @Override
      boolean compare(long left, long right) {
        return left < right;
      }

      @Override
      boolean compare(float left, float right) {
        return left < right;
      }

      @Override
      boolean compare(double left, double right) {
        return left < right;
      }
    };

    abstract boolean compare(int left, int right);

    abstract boolean compare(long left, long right);

    abstract boolean compare(float left, float right);

    abstract boolean compare(double left, double right);
  }

  @FunctionalInterface
  public interface DefaultValueWriter {
    void write(Partition partition, int channel, int index, ColumnBuilder builder);
  }

  @FunctionalInterface
  public interface IntermediateValueWriter {
    void write(
        GroupedMaxMinByBaseAccumulator accumulator,
        boolean isX,
        long index,
        byte[] bytes,
        int offset);
  }

  @FunctionalInterface
  public interface IntermediateValueInitializer {
    void initialize(GroupedMaxMinByBaseAccumulator accumulator, boolean isX);
  }
}
