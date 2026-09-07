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
import org.apache.iotdb.calc.execution.operator.process.fill.constant.BinaryConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.BooleanConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.DoubleConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.FloatConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.IntConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.LongConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.identity.IdentityFill;
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
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AbstractApproxPercentileAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.BinaryApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.BlobApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.BooleanApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.DoubleApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.FloatApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.IntApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.LongApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.approximate.HyperLogLog;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.AbstractGroupedApproxPercentileAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.BinaryGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.BlobGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.BooleanGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.DoubleGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.FloatGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedMaxMinByBaseAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.IntGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.LongGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.BinaryBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.BooleanBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.FloatBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.IntBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.plan.relational.planner.CastToBlobLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToBooleanLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToDateLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToDoubleLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToFloatLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToInt32LiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToInt64LiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToStringLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToTimestampLiteralVisitor;
import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.BinaryGreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.BinaryLeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.BooleanGreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.BooleanLeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.DoubleGreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.DoubleLeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.FloatGreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.FloatLeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.InBinaryMultiColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.InBooleanMultiColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.InDoubleMultiColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.InFloatMultiColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.InInt32MultiColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.InInt64MultiColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.InMultiColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.Int32GreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.Int32LeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.Int64GreatestColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.Int64LeastColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.util.CastFunctionUtils;
import org.apache.iotdb.calc.utils.datastructure.SortKey;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FloatLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.iotdb.calc.transformation.datastructure.util.BinaryUtils.MIN_ARRAY_HEADER_SIZE;
import static org.apache.iotdb.calc.transformation.datastructure.util.BinaryUtils.MIN_OBJECT_HEADER_SIZE;

public class TypeServices {

  private static final IdentityLinearFill IDENTITY_LINEAR_FILL = new IdentityLinearFill();
  private static final IdentityFill IDENTITY_FILL = new IdentityFill();

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

  public static final TypeService<ValueFillFactory> VALUE_FILL_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (literal, zoneId) -> {
                  Boolean value = literal.accept(new CastToBooleanLiteralVisitor(), null);
                  return value == null ? IDENTITY_FILL : new BooleanConstantFill(value);
                };
            case TEXT, STRING ->
                (literal, zoneId) -> {
                  Binary value =
                      literal.accept(
                          new CastToStringLiteralVisitor(TSFileConfig.STRING_CHARSET), null);
                  return value == null ? IDENTITY_FILL : new BinaryConstantFill(value);
                };
            case BLOB ->
                (literal, zoneId) -> {
                  Binary value = literal.accept(new CastToBlobLiteralVisitor(), null);
                  return value == null ? IDENTITY_FILL : new BinaryConstantFill(value);
                };
            case INT32 ->
                (literal, zoneId) -> {
                  Integer value = literal.accept(new CastToInt32LiteralVisitor(), null);
                  return value == null ? IDENTITY_FILL : new IntConstantFill(value);
                };
            case DATE ->
                (literal, zoneId) -> {
                  Integer value = literal.accept(new CastToDateLiteralVisitor(), null);
                  return value == null ? IDENTITY_FILL : new IntConstantFill(value);
                };
            case INT64 ->
                (literal, zoneId) -> {
                  Long value = literal.accept(new CastToInt64LiteralVisitor(), null);
                  return value == null ? IDENTITY_FILL : new LongConstantFill(value);
                };
            case TIMESTAMP ->
                (literal, zoneId) -> {
                  Long value = literal.accept(new CastToTimestampLiteralVisitor(zoneId), null);
                  return value == null ? IDENTITY_FILL : new LongConstantFill(value);
                };
            case FLOAT ->
                (literal, zoneId) -> {
                  Float value = literal.accept(new CastToFloatLiteralVisitor(), null);
                  return value == null ? IDENTITY_FILL : new FloatConstantFill(value);
                };
            case DOUBLE ->
                (literal, zoneId) -> {
                  Double value = literal.accept(new CastToDoubleLiteralVisitor(), null);
                  return value == null ? IDENTITY_FILL : new DoubleConstantFill(value);
                };
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (literal, zoneId) -> {
                  throw unsupportedDataType(type);
                };
          };

  /** Reads values for a cast using the source type's representation. */
  public static final TypeService<CastInputService> CAST_INPUT_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (column, columnBuilder, position, castValueService, zoneId) ->
                    castValueService.castInt(columnBuilder, type.getInt(column, position));
            case DATE ->
                (column, columnBuilder, position, castValueService, zoneId) ->
                    castValueService.castDate(columnBuilder, type.getInt(column, position), zoneId);
            case INT64 ->
                (column, columnBuilder, position, castValueService, zoneId) ->
                    castValueService.castLong(columnBuilder, type.getLong(column, position));
            case TIMESTAMP ->
                (column, columnBuilder, position, castValueService, zoneId) ->
                    castValueService.castTimestamp(
                        columnBuilder, type.getLong(column, position), zoneId);
            case FLOAT ->
                (column, columnBuilder, position, castValueService, zoneId) ->
                    castValueService.castFloat(columnBuilder, type.getFloat(column, position));
            case DOUBLE ->
                (column, columnBuilder, position, castValueService, zoneId) ->
                    castValueService.castDouble(columnBuilder, type.getDouble(column, position));
            case BOOLEAN ->
                (column, columnBuilder, position, castValueService, zoneId) ->
                    castValueService.castBoolean(columnBuilder, type.getBoolean(column, position));
            case TEXT, STRING ->
                (column, columnBuilder, position, castValueService, zoneId) ->
                    castValueService.castString(
                        columnBuilder, type.getBinary(column, position), zoneId);
            case BLOB ->
                (column, columnBuilder, position, castValueService, zoneId) ->
                    castValueService.castBlob(
                        columnBuilder, type.getBinary(column, position), zoneId);
            case OBJECT ->
                (column, columnBuilder, position, castValueService, zoneId) ->
                    castValueService.castObject(columnBuilder, type.getBinary(column, position));
            case ROW, UNKNOWN, VECTOR ->
                (column, columnBuilder, position, castValueService, zoneId) -> {
                  throw unsupportedCastSource(type);
                };
          };

  /** Writes values produced by a cast into a column using the target type's representation. */
  public static final TypeService<CastValueService> CAST_VALUE_SERVICE =
      type ->
          new CastValueService() {
            @Override
            public void castInt(ColumnBuilder columnBuilder, int value) {
              switch (type.getTypeEnum()) {
                case INT32, DATE -> type.writeInt(columnBuilder, value);
                case INT64, TIMESTAMP -> type.writeLong(columnBuilder, value);
                case FLOAT -> type.writeFloat(columnBuilder, value);
                case DOUBLE -> type.writeDouble(columnBuilder, value);
                case BOOLEAN -> type.writeBoolean(columnBuilder, value != 0);
                case TEXT, STRING ->
                    type.writeBinary(columnBuilder, BytesUtils.valueOf(String.valueOf(value)));
                case BLOB ->
                    type.writeBinary(columnBuilder, new Binary(BytesUtils.intToBytes(value)));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              }
            }

            @Override
            public void castDate(ColumnBuilder columnBuilder, int value, ZoneId zoneId) {
              switch (type.getTypeEnum()) {
                case INT32, DATE -> type.writeInt(columnBuilder, value);
                case INT64 -> type.writeLong(columnBuilder, value);
                case TIMESTAMP ->
                    type.writeLong(
                        columnBuilder,
                        DateTimeUtils.correctPrecision(
                            DateUtils.parseIntToTimestamp(value, zoneId)));
                case FLOAT -> type.writeFloat(columnBuilder, value);
                case DOUBLE -> type.writeDouble(columnBuilder, value);
                case BOOLEAN -> type.writeBoolean(columnBuilder, value != 0);
                case TEXT, STRING ->
                    type.writeBinary(
                        columnBuilder, BytesUtils.valueOf(DateUtils.formatDate(value)));
                case BLOB ->
                    type.writeBinary(columnBuilder, new Binary(BytesUtils.intToBytes(value)));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              }
            }

            @Override
            public void castTimestamp(ColumnBuilder columnBuilder, long value, ZoneId zoneId) {
              try {
                switch (type.getTypeEnum()) {
                  case INT32 ->
                      type.writeInt(columnBuilder, CastFunctionUtils.castLongToInt(value));
                  case DATE ->
                      type.writeInt(
                          columnBuilder,
                          DateUtils.parseDateExpressionToInt(
                              DateTimeUtils.convertToLocalDate(value, zoneId)));
                  case INT64, TIMESTAMP -> type.writeLong(columnBuilder, value);
                  case FLOAT -> type.writeFloat(columnBuilder, value);
                  case DOUBLE -> type.writeDouble(columnBuilder, value);
                  case BOOLEAN -> type.writeBoolean(columnBuilder, value != 0L);
                  case TEXT, STRING ->
                      type.writeBinary(
                          columnBuilder,
                          BytesUtils.valueOf(DateTimeUtils.convertLongToDate(value, zoneId)));
                  case BLOB ->
                      type.writeBinary(columnBuilder, new Binary(BytesUtils.longToBytes(value)));
                  case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
                }
              } catch (DateTimeParseException e) {
                throw new IoTDBRuntimeException(
                    "Year must be between 1000 and 9999.",
                    org.apache.iotdb.rpc.TSStatusCode.DATE_OUT_OF_RANGE.getStatusCode(),
                    true);
              }
            }

            @Override
            public void castLong(ColumnBuilder columnBuilder, long value) {
              switch (type.getTypeEnum()) {
                case INT32, DATE ->
                    type.writeInt(columnBuilder, CastFunctionUtils.castLongToInt(value));
                case INT64, TIMESTAMP -> type.writeLong(columnBuilder, value);
                case FLOAT -> type.writeFloat(columnBuilder, value);
                case DOUBLE -> type.writeDouble(columnBuilder, value);
                case BOOLEAN -> type.writeBoolean(columnBuilder, value != 0L);
                case TEXT, STRING ->
                    type.writeBinary(columnBuilder, BytesUtils.valueOf(String.valueOf(value)));
                case BLOB ->
                    type.writeBinary(columnBuilder, new Binary(BytesUtils.longToBytes(value)));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              }
            }

            @Override
            public void castFloat(ColumnBuilder columnBuilder, float value) {
              switch (type.getTypeEnum()) {
                case INT32, DATE ->
                    type.writeInt(columnBuilder, CastFunctionUtils.castFloatToInt(value));
                case INT64, TIMESTAMP ->
                    type.writeLong(columnBuilder, CastFunctionUtils.castFloatToLong(value));
                case FLOAT -> type.writeFloat(columnBuilder, value);
                case DOUBLE -> type.writeDouble(columnBuilder, value);
                case BOOLEAN -> type.writeBoolean(columnBuilder, value != 0.0f);
                case TEXT, STRING ->
                    type.writeBinary(columnBuilder, BytesUtils.valueOf(String.valueOf(value)));
                case BLOB ->
                    type.writeBinary(columnBuilder, new Binary(BytesUtils.floatToBytes(value)));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              }
            }

            @Override
            public void castDouble(ColumnBuilder columnBuilder, double value) {
              switch (type.getTypeEnum()) {
                case INT32, DATE ->
                    type.writeInt(columnBuilder, CastFunctionUtils.castDoubleToInt(value));
                case INT64, TIMESTAMP ->
                    type.writeLong(columnBuilder, CastFunctionUtils.castDoubleToLong(value));
                case FLOAT ->
                    type.writeFloat(columnBuilder, CastFunctionUtils.castDoubleToFloat(value));
                case DOUBLE -> type.writeDouble(columnBuilder, value);
                case BOOLEAN -> type.writeBoolean(columnBuilder, value != 0.0);
                case TEXT, STRING ->
                    type.writeBinary(columnBuilder, BytesUtils.valueOf(String.valueOf(value)));
                case BLOB ->
                    type.writeBinary(columnBuilder, new Binary(BytesUtils.doubleToBytes(value)));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              }
            }

            @Override
            public void castBoolean(ColumnBuilder columnBuilder, boolean value) {
              switch (type.getTypeEnum()) {
                case INT32, DATE -> type.writeInt(columnBuilder, value ? 1 : 0);
                case INT64, TIMESTAMP -> type.writeLong(columnBuilder, value ? 1L : 0L);
                case FLOAT -> type.writeFloat(columnBuilder, value ? 1.0f : 0);
                case DOUBLE -> type.writeDouble(columnBuilder, value ? 1.0 : 0);
                case BOOLEAN -> type.writeBoolean(columnBuilder, value);
                case TEXT, STRING ->
                    type.writeBinary(columnBuilder, BytesUtils.valueOf(String.valueOf(value)));
                case BLOB ->
                    type.writeBinary(columnBuilder, new Binary(BytesUtils.boolToBytes(value)));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              }
            }

            @Override
            public void castString(ColumnBuilder columnBuilder, Binary value, ZoneId zoneId) {
              String stringValue = value.getStringValue(TSFileConfig.STRING_CHARSET);
              try {
                switch (type.getTypeEnum()) {
                  case INT32 -> type.writeInt(columnBuilder, Integer.parseInt(stringValue));
                  case DATE ->
                      type.writeInt(columnBuilder, DateUtils.parseDateExpressionToInt(stringValue));
                  case INT64 -> type.writeLong(columnBuilder, Long.parseLong(stringValue));
                  case TIMESTAMP ->
                      type.writeLong(
                          columnBuilder,
                          DateTimeUtils.convertDatetimeStrToLong(stringValue, zoneId));
                  case FLOAT ->
                      type.writeFloat(
                          columnBuilder, CastFunctionUtils.castTextToFloat(stringValue));
                  case DOUBLE ->
                      type.writeDouble(
                          columnBuilder, CastFunctionUtils.castTextToDouble(stringValue));
                  case BOOLEAN ->
                      type.writeBoolean(
                          columnBuilder, CastFunctionUtils.castTextToBoolean(stringValue));
                  case TEXT, STRING, BLOB, OBJECT -> type.writeBinary(columnBuilder, value);
                  case ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
                }
              } catch (DateTimeParseException | NumberFormatException e) {
                throw new SemanticException(
                    String.format("Cannot cast %s to %s type", stringValue, type.getDisplayName()));
              }
            }

            @Override
            public void castBlob(ColumnBuilder columnBuilder, Binary value, ZoneId zoneId) {
              String stringValue = BytesUtils.parseBlobByteArrayToString(value.getValues());
              try {
                switch (type.getTypeEnum()) {
                  case INT32 -> type.writeInt(columnBuilder, Integer.parseInt(stringValue));
                  case DATE ->
                      type.writeInt(columnBuilder, DateUtils.parseDateExpressionToInt(stringValue));
                  case INT64 -> type.writeLong(columnBuilder, Long.parseLong(stringValue));
                  case TIMESTAMP ->
                      type.writeLong(
                          columnBuilder,
                          DateTimeUtils.convertDatetimeStrToLong(stringValue, zoneId));
                  case FLOAT ->
                      type.writeFloat(
                          columnBuilder, CastFunctionUtils.castTextToFloat(stringValue));
                  case DOUBLE ->
                      type.writeDouble(
                          columnBuilder, CastFunctionUtils.castTextToDouble(stringValue));
                  case BOOLEAN ->
                      type.writeBoolean(
                          columnBuilder, CastFunctionUtils.castTextToBoolean(stringValue));
                  case TEXT, STRING, BLOB -> type.writeBinary(columnBuilder, value);
                  case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
                }
              } catch (DateTimeParseException | NumberFormatException e) {
                throw new SemanticException(
                    String.format("Cannot cast %s to %s type", stringValue, type.getDisplayName()));
              }
            }

            @Override
            public void castObject(ColumnBuilder columnBuilder, Binary value) {
              String stringValue = BytesUtils.parseObjectByteArrayToString(value.getValues());
              switch (type.getTypeEnum()) {
                case STRING ->
                    type.writeBinary(
                        columnBuilder, BytesUtils.valueOf(String.valueOf(stringValue)));
                case INT32,
                    INT64,
                    FLOAT,
                    DOUBLE,
                    BOOLEAN,
                    DATE,
                    TIMESTAMP,
                    TEXT,
                    BLOB,
                    OBJECT,
                    ROW,
                    UNKNOWN,
                    VECTOR ->
                    throw unsupportedCastTarget(type);
              }
            }
          };

  public static final TypeService<CastObjectValueService> CAST_OBJECT_VALUE_SERVICE =
      type ->
          new CastObjectValueService() {
            @Override
            public Object castInt(int value) {
              return switch (type.getTypeEnum()) {
                case INT32, DATE -> value;
                case INT64, TIMESTAMP -> (long) value;
                case FLOAT -> (float) value;
                case DOUBLE -> (double) value;
                case BOOLEAN -> value != 0;
                case TEXT, STRING -> BytesUtils.valueOf(String.valueOf(value));
                case BLOB -> new Binary(BytesUtils.intToBytes(value));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              };
            }

            @Override
            public Object castDate(int value, ZoneId zoneId) {
              return switch (type.getTypeEnum()) {
                case INT32, DATE -> value;
                case INT64 -> (long) value;
                case TIMESTAMP ->
                    DateTimeUtils.correctPrecision(DateUtils.parseIntToTimestamp(value, zoneId));
                case FLOAT -> (float) value;
                case DOUBLE -> (double) value;
                case BOOLEAN -> value != 0;
                case TEXT, STRING -> BytesUtils.valueOf(DateUtils.formatDate(value));
                case BLOB -> new Binary(BytesUtils.intToBytes(value));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              };
            }

            @Override
            public Object castTimestamp(long value, ZoneId zoneId) {
              return switch (type.getTypeEnum()) {
                case INT32 -> CastFunctionUtils.castLongToInt(value);
                case DATE ->
                    DateUtils.parseDateExpressionToInt(
                        DateTimeUtils.convertToLocalDate(value, zoneId));
                case INT64, TIMESTAMP -> value;
                case FLOAT -> (float) value;
                case DOUBLE -> (double) value;
                case BOOLEAN -> value != 0L;
                case TEXT, STRING ->
                    BytesUtils.valueOf(DateTimeUtils.convertLongToDate(value, zoneId));
                case BLOB -> new Binary(BytesUtils.longToBytes(value));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              };
            }

            @Override
            public Object castLong(long value) {
              return switch (type.getTypeEnum()) {
                case INT32, DATE -> CastFunctionUtils.castLongToInt(value);
                case INT64, TIMESTAMP -> value;
                case FLOAT -> (float) value;
                case DOUBLE -> (double) value;
                case BOOLEAN -> value != 0L;
                case TEXT, STRING -> BytesUtils.valueOf(String.valueOf(value));
                case BLOB -> new Binary(BytesUtils.longToBytes(value));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              };
            }

            @Override
            public Object castFloat(float value) {
              return switch (type.getTypeEnum()) {
                case INT32, DATE -> CastFunctionUtils.castFloatToInt(value);
                case INT64, TIMESTAMP -> CastFunctionUtils.castFloatToLong(value);
                case FLOAT -> value;
                case DOUBLE -> (double) value;
                case BOOLEAN -> value != 0.0f;
                case TEXT, STRING -> BytesUtils.valueOf(String.valueOf(value));
                case BLOB -> new Binary(BytesUtils.floatToBytes(value));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              };
            }

            @Override
            public Object castDouble(double value) {
              return switch (type.getTypeEnum()) {
                case INT32, DATE -> CastFunctionUtils.castDoubleToInt(value);
                case INT64, TIMESTAMP -> CastFunctionUtils.castDoubleToLong(value);
                case FLOAT -> CastFunctionUtils.castDoubleToFloat(value);
                case DOUBLE -> value;
                case BOOLEAN -> value != 0.0d;
                case TEXT, STRING -> BytesUtils.valueOf(String.valueOf(value));
                case BLOB -> new Binary(BytesUtils.doubleToBytes(value));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              };
            }

            @Override
            public Object castBoolean(boolean value) {
              return switch (type.getTypeEnum()) {
                case INT32, DATE -> value ? 1 : 0;
                case INT64, TIMESTAMP -> value ? 1L : 0L;
                case FLOAT -> value ? 1.0f : 0.0f;
                case DOUBLE -> value ? 1.0d : 0.0d;
                case BOOLEAN -> value;
                case TEXT, STRING -> BytesUtils.valueOf(String.valueOf(value));
                case BLOB -> new Binary(BytesUtils.boolToBytes(value));
                case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
              };
            }

            @Override
            public Object castBinary(Binary value, ZoneId zoneId) {
              String stringValue = value.getStringValue(TSFileConfig.STRING_CHARSET);
              try {
                return switch (type.getTypeEnum()) {
                  case INT32 -> Integer.parseInt(stringValue);
                  case DATE -> DateUtils.parseDateExpressionToInt(stringValue);
                  case INT64 -> Long.parseLong(stringValue);
                  case TIMESTAMP -> DateTimeUtils.convertDatetimeStrToLong(stringValue, zoneId);
                  case FLOAT -> CastFunctionUtils.castTextToFloat(stringValue);
                  case DOUBLE -> CastFunctionUtils.castTextToDouble(stringValue);
                  case BOOLEAN -> CastFunctionUtils.castTextToBoolean(stringValue);
                  case TEXT, STRING, BLOB -> value;
                  case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedCastTarget(type);
                };
              } catch (DateTimeParseException | NumberFormatException e) {
                throw new SemanticException(
                    String.format("Cannot cast %s to %s type", stringValue, type.getDisplayName()));
              }
            }
          };

  public static final TypeService<CastObjectInputService> CAST_OBJECT_INPUT_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (value, castValueService, zoneId) ->
                    castValueService.castInt(
                        value instanceof Integer ? (int) value : ((Long) value).intValue());
            case DATE ->
                (value, castValueService, zoneId) ->
                    castValueService.castDate(
                        value instanceof Integer ? (int) value : ((Long) value).intValue(), zoneId);
            case INT64 ->
                (value, castValueService, zoneId) -> castValueService.castLong((Long) value);
            case TIMESTAMP ->
                (value, castValueService, zoneId) ->
                    castValueService.castTimestamp((Long) value, zoneId);
            case FLOAT ->
                (value, castValueService, zoneId) ->
                    castValueService.castFloat(
                        value instanceof Float ? (float) value : ((Double) value).floatValue());
            case DOUBLE ->
                (value, castValueService, zoneId) -> castValueService.castDouble((Double) value);
            case BOOLEAN ->
                (value, castValueService, zoneId) -> castValueService.castBoolean((Boolean) value);
            case TEXT, STRING, BLOB ->
                (value, castValueService, zoneId) ->
                    castValueService.castBinary((Binary) value, zoneId);
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (value, castValueService, zoneId) -> {
                  throw unsupportedCastSource(type);
                };
          };

  public static final TypeService<FormatValueConverter> FORMAT_VALUE_CONVERTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case UNKNOWN -> (column, position, zoneId) -> null;
            case INT32, INT64, FLOAT, DOUBLE, BOOLEAN, TEXT, STRING, BLOB ->
                (column, position, zoneId) -> column.getObject(position);
            // Java Formatter expects temporal objects for DATE and TIMESTAMP arguments.
            case DATE ->
                (column, position, zoneId) ->
                    DateUtils.parseIntToLocalDate(column.getInt(position));
            case TIMESTAMP ->
                (column, position, zoneId) ->
                    DateTimeUtils.convertToZonedDateTime(column.getLong(position), zoneId);
            case OBJECT, ROW, VECTOR ->
                (column, position, zoneId) -> {
                  throw new UnsupportedOperationException(
                      CalcMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  public static final TypeService<GroupedAccumulator>
      GROUPED_APPROX_MOST_FREQUENT_ACCUMULATOR_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN -> new BooleanGroupedApproxMostFrequentAccumulator();
                case INT32, DATE -> new IntGroupedApproxMostFrequentAccumulator();
                case INT64, TIMESTAMP -> new LongGroupedApproxMostFrequentAccumulator();
                case FLOAT -> new FloatGroupedApproxMostFrequentAccumulator();
                case DOUBLE -> new DoubleGroupedApproxMostFrequentAccumulator();
                case TEXT, STRING -> new BinaryGroupedApproxMostFrequentAccumulator();
                case BLOB -> new BlobGroupedApproxMostFrequentAccumulator();
                case OBJECT, ROW, UNKNOWN, VECTOR ->
                    throw unsupportedApproxMostFrequentDataType(type);
              };

  public static final TypeService<TableAccumulator> APPROX_MOST_FREQUENT_ACCUMULATOR_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> new BooleanApproxMostFrequentAccumulator();
            case INT32, DATE -> new IntApproxMostFrequentAccumulator();
            case INT64, TIMESTAMP -> new LongApproxMostFrequentAccumulator();
            case FLOAT -> new FloatApproxMostFrequentAccumulator();
            case DOUBLE -> new DoubleApproxMostFrequentAccumulator();
            case TEXT, STRING -> new BinaryApproxMostFrequentAccumulator();
            case BLOB -> new BlobApproxMostFrequentAccumulator();
            case OBJECT, ROW, UNKNOWN, VECTOR -> throw unsupportedApproxMostFrequentDataType(type);
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

  /** Writes a statistics value into a reusable primitive result. */
  public static final TypeService<StatisticsValueSetter> STATISTICS_VALUE_SETTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (target, value, exceptionSupplier) -> target.setInt(((Number) value).intValue());
            case INT64, TIMESTAMP ->
                (target, value, exceptionSupplier) -> target.setLong(((Number) value).longValue());
            case FLOAT ->
                (target, value, exceptionSupplier) ->
                    target.setFloat(((Number) value).floatValue());
            case DOUBLE ->
                (target, value, exceptionSupplier) ->
                    target.setDouble(((Number) value).doubleValue());
            case BOOLEAN ->
                (target, value, exceptionSupplier) -> target.setBoolean((boolean) value);
            case TEXT, STRING, BLOB, OBJECT ->
                (target, value, exceptionSupplier) -> target.setBinary(toBinary(value));
            case ROW, UNKNOWN, VECTOR ->
                (target, value, exceptionSupplier) -> {
                  throw exceptionSupplier.get();
                };
          };

  public static final TypeService<InColumnValueMatcherFactory> IN_COLUMN_VALUE_MATCHER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                values -> {
                  Set<Integer> intSet = new HashSet<>();
                  for (String value : values) {
                    try {
                      intSet.add(Integer.valueOf(value));
                    } catch (IllegalArgumentException e) {
                      throw new SemanticException(
                          String.format(
                              CalcMessages.CANNOT_CAST_TO_TYPE, value, type.getTypeEnum()));
                    }
                  }
                  return (column, position) -> intSet.contains(type.getInt(column, position));
                };
            case INT64, TIMESTAMP ->
                values -> {
                  Set<Long> longSet = new HashSet<>();
                  for (String value : values) {
                    try {
                      longSet.add(Long.valueOf(value));
                    } catch (IllegalArgumentException e) {
                      throw new SemanticException(
                          String.format(
                              CalcMessages.CANNOT_CAST_TO_TYPE, value, type.getTypeEnum()));
                    }
                  }
                  return (column, position) -> longSet.contains(type.getLong(column, position));
                };
            case FLOAT ->
                values -> {
                  Set<Float> floatSet = new HashSet<>();
                  for (String value : values) {
                    try {
                      floatSet.add(Float.valueOf(value));
                    } catch (IllegalArgumentException e) {
                      throw new SemanticException(
                          String.format(
                              CalcMessages.CANNOT_CAST_TO_TYPE, value, type.getTypeEnum()));
                    }
                  }
                  return (column, position) -> floatSet.contains(type.getFloat(column, position));
                };
            case DOUBLE ->
                values -> {
                  Set<Double> doubleSet = new HashSet<>();
                  for (String value : values) {
                    try {
                      doubleSet.add(Double.valueOf(value));
                    } catch (IllegalArgumentException e) {
                      throw new SemanticException(
                          String.format(
                              CalcMessages.CANNOT_CAST_TO_TYPE, value, type.getTypeEnum()));
                    }
                  }
                  return (column, position) -> doubleSet.contains(type.getDouble(column, position));
                };
            case BOOLEAN ->
                values -> {
                  Set<Boolean> booleanSet = new HashSet<>();
                  for (String value : values) {
                    if ("true".equalsIgnoreCase(value)) {
                      booleanSet.add(true);
                    } else if ("false".equalsIgnoreCase(value)) {
                      booleanSet.add(false);
                    } else {
                      throw new SemanticException(
                          String.format(CalcMessages.CANNOT_CAST_TO_BOOLEAN, value));
                    }
                  }
                  return (column, position) ->
                      booleanSet.contains(type.getBoolean(column, position));
                };
            case TEXT, STRING ->
                values -> {
                  Set<Binary> stringSet =
                      values.stream()
                          .map(value -> new Binary(value, TSFileConfig.STRING_CHARSET))
                          .collect(Collectors.toSet());
                  return (column, position) -> stringSet.contains(type.getBinary(column, position));
                };
            case BLOB, DATE, OBJECT, ROW, UNKNOWN, VECTOR ->
                values -> {
                  throw new UnsupportedOperationException(
                      CalcMessages.UNSUPPORTED_DATA_TYPE_LOWER + type.getTypeEnum());
                };
          };

  public static final TypeService<InMultiColumnTransformerFactory>
      IN_MULTI_COLUMN_TRANSFORMER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32 ->
                    (valueColumnTransformers, values) -> {
                      Set<Integer> intSet = new HashSet<>();
                      for (Literal value : values) {
                        try {
                          long v = ((LongLiteral) value).getParsedValue();
                          if (v <= Integer.MAX_VALUE && v >= Integer.MIN_VALUE) {
                            intSet.add((int) v);
                          }
                        } catch (IllegalArgumentException e) {
                          throw new SemanticException(
                              String.format(
                                  CalcMessages.CANNOT_CAST_TO_TYPE, value, type.getTypeEnum()));
                        }
                      }
                      return new InInt32MultiColumnTransformer(intSet, valueColumnTransformers);
                    };
                case DATE ->
                    (valueColumnTransformers, values) -> {
                      Set<Integer> dateSet = new HashSet<>();
                      for (Literal value : values) {
                        dateSet.add(Integer.parseInt(((GenericLiteral) value).getValue()));
                      }
                      return new InInt32MultiColumnTransformer(dateSet, valueColumnTransformers);
                    };
                case INT64 ->
                    (valueColumnTransformers, values) -> {
                      Set<Long> longSet = new HashSet<>();
                      for (Literal value : values) {
                        longSet.add(((LongLiteral) value).getParsedValue());
                      }
                      return new InInt64MultiColumnTransformer(longSet, valueColumnTransformers);
                    };
                case TIMESTAMP ->
                    (valueColumnTransformers, values) -> {
                      Set<Long> timestampSet = new HashSet<>();
                      for (Literal value : values) {
                        try {
                          if (value instanceof LongLiteral) {
                            timestampSet.add(((LongLiteral) value).getParsedValue());
                          } else if (value instanceof DoubleLiteral) {
                            timestampSet.add((long) ((DoubleLiteral) value).getValue());
                          } else if (value instanceof FloatLiteral) {
                            timestampSet.add((long) ((FloatLiteral) value).getValue());
                          } else if (value instanceof GenericLiteral) {
                            timestampSet.add(Long.parseLong(((GenericLiteral) value).getValue()));
                          } else {
                            throw new SemanticException(
                                String.format(
                                    CalcMessages.IN_LIST_LITERAL_FOR_TIMESTAMP_TYPE_RESTRICTION,
                                    value.getClass().getSimpleName()));
                          }
                        } catch (IllegalArgumentException e) {
                          throw new SemanticException(
                              String.format(
                                  CalcMessages.CANNOT_CAST_TO_TYPE, value, type.getTypeEnum()));
                        }
                      }
                      return new InInt64MultiColumnTransformer(
                          timestampSet, valueColumnTransformers);
                    };
                case FLOAT ->
                    (valueColumnTransformers, values) -> {
                      Set<Float> floatSet = new HashSet<>();
                      for (Literal value : values) {
                        try {
                          if (value instanceof FloatLiteral) {
                            floatSet.add(((FloatLiteral) value).getValue());
                          } else {
                            floatSet.add((float) ((DoubleLiteral) value).getValue());
                          }
                        } catch (IllegalArgumentException e) {
                          throw new SemanticException(
                              String.format(
                                  CalcMessages.CANNOT_CAST_TO_TYPE, value, type.getTypeEnum()));
                        }
                      }
                      return new InFloatMultiColumnTransformer(floatSet, valueColumnTransformers);
                    };
                case DOUBLE ->
                    (valueColumnTransformers, values) -> {
                      Set<Double> doubleSet = new HashSet<>();
                      for (Literal value : values) {
                        try {
                          if (value instanceof FloatLiteral) {
                            doubleSet.add((double) ((FloatLiteral) value).getValue());
                          } else {
                            doubleSet.add(((DoubleLiteral) value).getValue());
                          }
                        } catch (IllegalArgumentException e) {
                          throw new SemanticException(
                              String.format(
                                  CalcMessages.CANNOT_CAST_TO_TYPE, value, type.getTypeEnum()));
                        }
                      }
                      return new InDoubleMultiColumnTransformer(doubleSet, valueColumnTransformers);
                    };
                case BOOLEAN ->
                    (valueColumnTransformers, values) -> {
                      Set<Boolean> booleanSet = new HashSet<>();
                      for (Literal value : values) {
                        booleanSet.add(((BooleanLiteral) value).getValue());
                      }
                      return new InBooleanMultiColumnTransformer(
                          booleanSet, valueColumnTransformers);
                    };
                case TEXT, STRING ->
                    (valueColumnTransformers, values) -> {
                      Set<Binary> stringSet = new HashSet<>();
                      for (Literal value : values) {
                        stringSet.add(
                            new Binary(
                                ((StringLiteral) value).getValue(), TSFileConfig.STRING_CHARSET));
                      }
                      return new InBinaryMultiColumnTransformer(stringSet, valueColumnTransformers);
                    };
                case BLOB ->
                    (valueColumnTransformers, values) -> {
                      Set<Binary> binarySet = new HashSet<>();
                      for (Literal value : values) {
                        binarySet.add(new Binary(((BinaryLiteral) value).getValue()));
                      }
                      return new InBinaryMultiColumnTransformer(binarySet, valueColumnTransformers);
                    };
                case OBJECT, ROW, UNKNOWN, VECTOR ->
                    (valueColumnTransformers, values) -> {
                      throw new UnsupportedOperationException(
                          CalcMessages.UNSUPPORTED_DATA_TYPE_LOWER + type.getTypeEnum());
                    };
              };

  /** Updates a result with the minimum value from statistics. */
  public static final TypeService<StatisticsValueUpdater> MIN_STATISTICS_VALUE_UPDATER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (result, value, initialized, exceptionSupplier) -> {
                  int candidate = ((Number) value).intValue();
                  if (!initialized || candidate < result.getInt()) {
                    result.setInt(candidate);
                    return true;
                  }
                  return false;
                };
            case INT64, TIMESTAMP ->
                (result, value, initialized, exceptionSupplier) -> {
                  long candidate = ((Number) value).longValue();
                  if (!initialized || candidate < result.getLong()) {
                    result.setLong(candidate);
                    return true;
                  }
                  return false;
                };
            case FLOAT ->
                (result, value, initialized, exceptionSupplier) -> {
                  float candidate = ((Number) value).floatValue();
                  if (!initialized || candidate < result.getFloat()) {
                    result.setFloat(candidate);
                    return true;
                  }
                  return false;
                };
            case DOUBLE ->
                (result, value, initialized, exceptionSupplier) -> {
                  double candidate = ((Number) value).doubleValue();
                  if (!initialized || candidate < result.getDouble()) {
                    result.setDouble(candidate);
                    return true;
                  }
                  return false;
                };
            case BOOLEAN ->
                (result, value, initialized, exceptionSupplier) -> {
                  boolean candidate = (boolean) value;
                  if (!initialized || !candidate) {
                    result.setBoolean(candidate);
                    return true;
                  }
                  return false;
                };
            case TEXT, STRING, BLOB ->
                (result, value, initialized, exceptionSupplier) -> {
                  Binary candidate = toBinary(value);
                  if (!initialized || candidate.compareTo(result.getBinary()) < 0) {
                    result.setBinary(candidate);
                    return true;
                  }
                  return false;
                };
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (result, value, initialized, exceptionSupplier) -> {
                  throw exceptionSupplier.get();
                };
          };

  /** Updates a result with the maximum value from statistics. */
  public static final TypeService<StatisticsValueUpdater> MAX_STATISTICS_VALUE_UPDATER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (result, value, initialized, exceptionSupplier) -> {
                  int candidate = ((Number) value).intValue();
                  if (!initialized || candidate > result.getInt()) {
                    result.setInt(candidate);
                    return true;
                  }
                  return false;
                };
            case INT64, TIMESTAMP ->
                (result, value, initialized, exceptionSupplier) -> {
                  long candidate = ((Number) value).longValue();
                  if (!initialized || candidate > result.getLong()) {
                    result.setLong(candidate);
                    return true;
                  }
                  return false;
                };
            case FLOAT ->
                (result, value, initialized, exceptionSupplier) -> {
                  float candidate = ((Number) value).floatValue();
                  if (!initialized || candidate > result.getFloat()) {
                    result.setFloat(candidate);
                    return true;
                  }
                  return false;
                };
            case DOUBLE ->
                (result, value, initialized, exceptionSupplier) -> {
                  double candidate = ((Number) value).doubleValue();
                  if (!initialized || candidate > result.getDouble()) {
                    result.setDouble(candidate);
                    return true;
                  }
                  return false;
                };
            case BOOLEAN ->
                (result, value, initialized, exceptionSupplier) -> {
                  boolean candidate = (boolean) value;
                  if (!initialized || candidate) {
                    result.setBoolean(candidate);
                    return true;
                  }
                  return false;
                };
            case TEXT, STRING, BLOB ->
                (result, value, initialized, exceptionSupplier) -> {
                  Binary candidate = toBinary(value);
                  if (!initialized || candidate.compareTo(result.getBinary()) > 0) {
                    result.setBinary(candidate);
                    return true;
                  }
                  return false;
                };
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (result, value, initialized, exceptionSupplier) -> {
                  throw exceptionSupplier.get();
                };
          };

  /** Updates a result with the greatest absolute numeric value from statistics. */
  public static final TypeService<StatisticsValueUpdater> EXTREME_STATISTICS_VALUE_UPDATER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                (result, value, initialized, exceptionSupplier) -> {
                  int candidate = ((Number) value).intValue();
                  if (!initialized || compareExtreme(candidate, result.getInt()) > 0) {
                    result.setInt(candidate);
                    return true;
                  }
                  return false;
                };
            case INT64 ->
                (result, value, initialized, exceptionSupplier) -> {
                  long candidate = ((Number) value).longValue();
                  if (!initialized || compareExtreme(candidate, result.getLong()) > 0) {
                    result.setLong(candidate);
                    return true;
                  }
                  return false;
                };
            case FLOAT ->
                (result, value, initialized, exceptionSupplier) -> {
                  float candidate = ((Number) value).floatValue();
                  float absCandidate = Math.abs(candidate);
                  float absResult = Math.abs(result.getFloat());
                  if (!initialized
                      || absCandidate > absResult
                      || absCandidate == absResult && candidate > result.getFloat()) {
                    result.setFloat(candidate);
                    return true;
                  }
                  return false;
                };
            case DOUBLE ->
                (result, value, initialized, exceptionSupplier) -> {
                  double candidate = ((Number) value).doubleValue();
                  double absCandidate = Math.abs(candidate);
                  double absResult = Math.abs(result.getDouble());
                  if (!initialized
                      || absCandidate > absResult
                      || absCandidate == absResult && candidate > result.getDouble()) {
                    result.setDouble(candidate);
                    return true;
                  }
                  return false;
                };
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                (result, value, initialized, exceptionSupplier) -> {
                  throw exceptionSupplier.get();
                };
          };

  /** Binds an APPROX_PERCENTILE input reader to the input column's native type. */
  public static final TypeService<
          Function<AbstractApproxPercentileAccumulator, BiConsumer<Column[], AggregationMask>>>
      APPROX_PERCENTILE_INPUT_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32 -> accumulator -> accumulator::addIntInput;
                case INT64, TIMESTAMP -> accumulator -> accumulator::addLongInput;
                case FLOAT -> accumulator -> accumulator::addFloatInput;
                case DOUBLE -> accumulator -> accumulator::addDoubleInput;
                case BOOLEAN, DATE, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                    accumulator ->
                        (arguments, mask) -> {
                          throw new UnSupportedDataTypeException(
                              String.format(
                                  CalcMessages
                                      .UNSUPPORTED_DATA_TYPE_IN_APPROX_PERCENTILE_AGGREGATION,
                                  type.getTypeEnum()));
                        };
              };

  /** Binds a grouped APPROX_PERCENTILE input reader to the input column's native type. */
  public static final TypeService<
          Function<
              AbstractGroupedApproxPercentileAccumulator,
              TriConsumer<int[], Column[], AggregationMask>>>
      GROUPED_APPROX_PERCENTILE_INPUT_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32 -> accumulator -> accumulator::addIntInput;
                case INT64, TIMESTAMP -> accumulator -> accumulator::addLongInput;
                case FLOAT -> accumulator -> accumulator::addFloatInput;
                case DOUBLE -> accumulator -> accumulator::addDoubleInput;
                case BOOLEAN, DATE, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                    accumulator ->
                        (groupIds, arguments, mask) -> {
                          throw new UnSupportedDataTypeException(
                              String.format(
                                  CalcMessages
                                      .UNSUPPORTED_DATA_TYPE_IN_APPROX_PERCENTILE_AGGREGATION,
                                  type.getTypeEnum()));
                        };
              };

  /** Provides storage operations for grouped accumulators backed by primitive big arrays. */
  public static final TypeService<GroupedValueService> GROUPED_VALUE_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                new GroupedValueService() {
                  @Override
                  public void initialize(GroupedValueAccessor accessor) {
                    accessor.initializeIntValues();
                  }

                  @Override
                  public void ensureCapacity(GroupedValueAccessor accessor, long groupCount) {
                    accessor.getIntValues().ensureCapacity(groupCount);
                  }

                  @Override
                  public long sizeOf(GroupedValueAccessor accessor) {
                    return accessor.getIntValues().sizeOf();
                  }

                  @Override
                  public void reset(GroupedValueAccessor accessor) {
                    accessor.getIntValues().reset();
                  }

                  @Override
                  public void write(
                      GroupedValueAccessor accessor, int groupId, ColumnBuilder columnBuilder) {
                    columnBuilder.writeInt(accessor.getIntValues().get(groupId));
                  }
                };
            case INT64, TIMESTAMP ->
                new GroupedValueService() {
                  @Override
                  public void initialize(GroupedValueAccessor accessor) {
                    accessor.initializeLongValues();
                  }

                  @Override
                  public void ensureCapacity(GroupedValueAccessor accessor, long groupCount) {
                    accessor.getLongValues().ensureCapacity(groupCount);
                  }

                  @Override
                  public long sizeOf(GroupedValueAccessor accessor) {
                    return accessor.getLongValues().sizeOf();
                  }

                  @Override
                  public void reset(GroupedValueAccessor accessor) {
                    accessor.getLongValues().reset();
                  }

                  @Override
                  public void write(
                      GroupedValueAccessor accessor, int groupId, ColumnBuilder columnBuilder) {
                    columnBuilder.writeLong(accessor.getLongValues().get(groupId));
                  }
                };
            case FLOAT ->
                new GroupedValueService() {
                  @Override
                  public void initialize(GroupedValueAccessor accessor) {
                    accessor.initializeFloatValues();
                  }

                  @Override
                  public void ensureCapacity(GroupedValueAccessor accessor, long groupCount) {
                    accessor.getFloatValues().ensureCapacity(groupCount);
                  }

                  @Override
                  public long sizeOf(GroupedValueAccessor accessor) {
                    return accessor.getFloatValues().sizeOf();
                  }

                  @Override
                  public void reset(GroupedValueAccessor accessor) {
                    accessor.getFloatValues().reset();
                  }

                  @Override
                  public void write(
                      GroupedValueAccessor accessor, int groupId, ColumnBuilder columnBuilder) {
                    columnBuilder.writeFloat(accessor.getFloatValues().get(groupId));
                  }
                };
            case DOUBLE ->
                new GroupedValueService() {
                  @Override
                  public void initialize(GroupedValueAccessor accessor) {
                    accessor.initializeDoubleValues();
                  }

                  @Override
                  public void ensureCapacity(GroupedValueAccessor accessor, long groupCount) {
                    accessor.getDoubleValues().ensureCapacity(groupCount);
                  }

                  @Override
                  public long sizeOf(GroupedValueAccessor accessor) {
                    return accessor.getDoubleValues().sizeOf();
                  }

                  @Override
                  public void reset(GroupedValueAccessor accessor) {
                    accessor.getDoubleValues().reset();
                  }

                  @Override
                  public void write(
                      GroupedValueAccessor accessor, int groupId, ColumnBuilder columnBuilder) {
                    columnBuilder.writeDouble(accessor.getDoubleValues().get(groupId));
                  }
                };
            case TEXT, STRING, BLOB, OBJECT ->
                new GroupedValueService() {
                  @Override
                  public void initialize(GroupedValueAccessor accessor) {
                    accessor.initializeBinaryValues();
                  }

                  @Override
                  public void ensureCapacity(GroupedValueAccessor accessor, long groupCount) {
                    accessor.getBinaryValues().ensureCapacity(groupCount);
                  }

                  @Override
                  public long sizeOf(GroupedValueAccessor accessor) {
                    return accessor.getBinaryValues().sizeOf();
                  }

                  @Override
                  public void reset(GroupedValueAccessor accessor) {
                    accessor.getBinaryValues().reset();
                  }

                  @Override
                  public void write(
                      GroupedValueAccessor accessor, int groupId, ColumnBuilder columnBuilder) {
                    columnBuilder.writeBinary(accessor.getBinaryValues().get(groupId));
                  }
                };
            case BOOLEAN ->
                new GroupedValueService() {
                  @Override
                  public void initialize(GroupedValueAccessor accessor) {
                    accessor.initializeBooleanValues();
                  }

                  @Override
                  public void ensureCapacity(GroupedValueAccessor accessor, long groupCount) {
                    accessor.getBooleanValues().ensureCapacity(groupCount);
                  }

                  @Override
                  public long sizeOf(GroupedValueAccessor accessor) {
                    return accessor.getBooleanValues().sizeOf();
                  }

                  @Override
                  public void reset(GroupedValueAccessor accessor) {
                    accessor.getBooleanValues().reset();
                  }

                  @Override
                  public void write(
                      GroupedValueAccessor accessor, int groupId, ColumnBuilder columnBuilder) {
                    columnBuilder.writeBoolean(accessor.getBooleanValues().get(groupId));
                  }
                };
            case ROW, UNKNOWN, VECTOR -> unsupportedGroupedValueService(type);
          };

  /** Reads grouped input and intermediate columns using the native column accessor. */
  public static final TypeService<GroupedInputReader> GROUPED_INPUT_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                new GroupedInputReader() {
                  @Override
                  public void addInput(
                      int[] groupIds,
                      Column[] arguments,
                      AggregationMask mask,
                      GroupedInputConsumer consumer) {
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateInt(groupIds[position], arguments[0].getInt(position)));
                  }

                  @Override
                  public void addIntermediate(
                      int[] groupIds, Column argument, GroupedInputConsumer consumer) {
                    for (int i = 0; i < groupIds.length; i++) {
                      if (!argument.isNull(i)) {
                        consumer.updateInt(groupIds[i], argument.getInt(i));
                      }
                    }
                  }
                };
            case INT64, TIMESTAMP ->
                new GroupedInputReader() {
                  @Override
                  public void addInput(
                      int[] groupIds,
                      Column[] arguments,
                      AggregationMask mask,
                      GroupedInputConsumer consumer) {
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateLong(
                                groupIds[position], arguments[0].getLong(position)));
                  }

                  @Override
                  public void addIntermediate(
                      int[] groupIds, Column argument, GroupedInputConsumer consumer) {
                    for (int i = 0; i < groupIds.length; i++) {
                      if (!argument.isNull(i)) {
                        consumer.updateLong(groupIds[i], argument.getLong(i));
                      }
                    }
                  }
                };
            case FLOAT ->
                new GroupedInputReader() {
                  @Override
                  public void addInput(
                      int[] groupIds,
                      Column[] arguments,
                      AggregationMask mask,
                      GroupedInputConsumer consumer) {
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateFloat(
                                groupIds[position], arguments[0].getFloat(position)));
                  }

                  @Override
                  public void addIntermediate(
                      int[] groupIds, Column argument, GroupedInputConsumer consumer) {
                    for (int i = 0; i < groupIds.length; i++) {
                      if (!argument.isNull(i)) {
                        consumer.updateFloat(groupIds[i], argument.getFloat(i));
                      }
                    }
                  }
                };
            case DOUBLE ->
                new GroupedInputReader() {
                  @Override
                  public void addInput(
                      int[] groupIds,
                      Column[] arguments,
                      AggregationMask mask,
                      GroupedInputConsumer consumer) {
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateDouble(
                                groupIds[position], arguments[0].getDouble(position)));
                  }

                  @Override
                  public void addIntermediate(
                      int[] groupIds, Column argument, GroupedInputConsumer consumer) {
                    for (int i = 0; i < groupIds.length; i++) {
                      if (!argument.isNull(i)) {
                        consumer.updateDouble(groupIds[i], argument.getDouble(i));
                      }
                    }
                  }
                };
            case TEXT, STRING, BLOB, OBJECT ->
                new GroupedInputReader() {
                  @Override
                  public void addInput(
                      int[] groupIds,
                      Column[] arguments,
                      AggregationMask mask,
                      GroupedInputConsumer consumer) {
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateBinary(
                                groupIds[position], arguments[0].getBinary(position)));
                  }

                  @Override
                  public void addIntermediate(
                      int[] groupIds, Column argument, GroupedInputConsumer consumer) {
                    for (int i = 0; i < groupIds.length; i++) {
                      if (!argument.isNull(i)) {
                        consumer.updateBinary(groupIds[i], argument.getBinary(i));
                      }
                    }
                  }
                };
            case BOOLEAN ->
                new GroupedInputReader() {
                  @Override
                  public void addInput(
                      int[] groupIds,
                      Column[] arguments,
                      AggregationMask mask,
                      GroupedInputConsumer consumer) {
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateBoolean(
                                groupIds[position], arguments[0].getBoolean(position)));
                  }

                  @Override
                  public void addIntermediate(
                      int[] groupIds, Column argument, GroupedInputConsumer consumer) {
                    for (int i = 0; i < groupIds.length; i++) {
                      if (!argument.isNull(i)) {
                        consumer.updateBoolean(groupIds[i], argument.getBoolean(i));
                      }
                    }
                  }
                };
            case ROW, UNKNOWN, VECTOR -> unsupportedGroupedInputReader(type);
          };

  /** Restricts grouped EXTREME to the numeric types supported by the aggregate. */
  public static final TypeService<GroupedValueService> GROUPED_EXTREME_VALUE_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, INT64, FLOAT, DOUBLE -> GROUPED_VALUE_SERVICE.call(type);
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                unsupportedGroupedValueService(type);
          };

  public static final TypeService<GroupedInputReader> GROUPED_EXTREME_INPUT_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, INT64, FLOAT, DOUBLE -> GROUPED_INPUT_READER_SERVICE.call(type);
            case BOOLEAN, DATE, TIMESTAMP, TEXT, STRING, BLOB, OBJECT, ROW, UNKNOWN, VECTOR ->
                unsupportedGroupedInputReader(type);
          };

  /** Reads a value column together with its optional ordering-time column. */
  public static final TypeService<GroupedTimeInputReader> GROUPED_TIME_INPUT_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateInt(
                                groupIds[position],
                                arguments[0].getInt(position),
                                arguments[1].isNull(position),
                                arguments[1].isNull(position)
                                    ? 0
                                    : arguments[1].getLong(position)));
            case INT64, TIMESTAMP ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateLong(
                                groupIds[position],
                                arguments[0].getLong(position),
                                arguments[1].isNull(position),
                                arguments[1].isNull(position)
                                    ? 0
                                    : arguments[1].getLong(position)));
            case FLOAT ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateFloat(
                                groupIds[position],
                                arguments[0].getFloat(position),
                                arguments[1].isNull(position),
                                arguments[1].isNull(position)
                                    ? 0
                                    : arguments[1].getLong(position)));
            case DOUBLE ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateDouble(
                                groupIds[position],
                                arguments[0].getDouble(position),
                                arguments[1].isNull(position),
                                arguments[1].isNull(position)
                                    ? 0
                                    : arguments[1].getLong(position)));
            case TEXT, STRING, BLOB, OBJECT ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateBinary(
                                groupIds[position],
                                arguments[0].getBinary(position),
                                arguments[1].isNull(position),
                                arguments[1].isNull(position)
                                    ? 0
                                    : arguments[1].getLong(position)));
            case BOOLEAN ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[0],
                        mask,
                        position ->
                            consumer.updateBoolean(
                                groupIds[position],
                                arguments[0].getBoolean(position),
                                arguments[1].isNull(position),
                                arguments[1].isNull(position)
                                    ? 0
                                    : arguments[1].getLong(position)));
            case ROW, UNKNOWN, VECTOR ->
                (groupIds, arguments, mask, consumer) -> {
                  throw unsupportedDataType(type);
                };
          };

  /** Reads x values for FIRST_BY/LAST_BY while filtering null y values. */
  public static final TypeService<GroupedByInputReader> GROUPED_BY_INPUT_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            consumer.updateInt(
                                groupIds[position],
                                arguments[0].isNull(position),
                                arguments[0].getInt(position),
                                arguments[2].isNull(position),
                                arguments[2].isNull(position)
                                    ? 0
                                    : arguments[2].getLong(position)));
            case INT64, TIMESTAMP ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            consumer.updateLong(
                                groupIds[position],
                                arguments[0].isNull(position),
                                arguments[0].getLong(position),
                                arguments[2].isNull(position),
                                arguments[2].isNull(position)
                                    ? 0
                                    : arguments[2].getLong(position)));
            case FLOAT ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            consumer.updateFloat(
                                groupIds[position],
                                arguments[0].isNull(position),
                                arguments[0].getFloat(position),
                                arguments[2].isNull(position),
                                arguments[2].isNull(position)
                                    ? 0
                                    : arguments[2].getLong(position)));
            case DOUBLE ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            consumer.updateDouble(
                                groupIds[position],
                                arguments[0].isNull(position),
                                arguments[0].getDouble(position),
                                arguments[2].isNull(position),
                                arguments[2].isNull(position)
                                    ? 0
                                    : arguments[2].getLong(position)));
            case TEXT, STRING, BLOB, OBJECT ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            consumer.updateBinary(
                                groupIds[position],
                                arguments[0].isNull(position),
                                arguments[0].getBinary(position),
                                arguments[2].isNull(position),
                                arguments[2].isNull(position)
                                    ? 0
                                    : arguments[2].getLong(position)));
            case BOOLEAN ->
                (groupIds, arguments, mask, consumer) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            consumer.updateBoolean(
                                groupIds[position],
                                arguments[0].isNull(position),
                                arguments[0].getBoolean(position),
                                arguments[2].isNull(position),
                                arguments[2].isNull(position)
                                    ? 0
                                    : arguments[2].getLong(position)));
            case ROW, UNKNOWN, VECTOR ->
                (groupIds, arguments, mask, consumer) -> {
                  throw unsupportedDataType(type);
                };
          };

  /** Serializes/deserializes a grouped value in the native intermediate format. */
  public static final TypeService<GroupedValueSerializer> GROUPED_VALUE_SERIALIZER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                new GroupedValueSerializer() {
                  @Override
                  public int calcTypeSize(GroupedValueAccessor accessor, long index) {
                    return Integer.BYTES;
                  }

                  @Override
                  public void serialize(
                      GroupedValueAccessor accessor, long index, byte[] bytes, int offset) {
                    accessor.getIntValues().toBytes(index, bytes, offset);
                  }
                };
            case INT64, TIMESTAMP ->
                new GroupedValueSerializer() {
                  @Override
                  public int calcTypeSize(GroupedValueAccessor accessor, long index) {
                    return Long.BYTES;
                  }

                  @Override
                  public void serialize(
                      GroupedValueAccessor accessor, long index, byte[] bytes, int offset) {
                    accessor.getLongValues().toBytes(index, bytes, offset);
                  }
                };
            case FLOAT ->
                new GroupedValueSerializer() {
                  @Override
                  public int calcTypeSize(GroupedValueAccessor accessor, long index) {
                    return Float.BYTES;
                  }

                  @Override
                  public void serialize(
                      GroupedValueAccessor accessor, long index, byte[] bytes, int offset) {
                    accessor.getFloatValues().toBytes(index, bytes, offset);
                  }
                };
            case DOUBLE ->
                new GroupedValueSerializer() {
                  @Override
                  public int calcTypeSize(GroupedValueAccessor accessor, long index) {
                    return Double.BYTES;
                  }

                  @Override
                  public void serialize(
                      GroupedValueAccessor accessor, long index, byte[] bytes, int offset) {
                    accessor.getDoubleValues().toBytes(index, bytes, offset);
                  }
                };
            case TEXT, STRING, BLOB, OBJECT ->
                new GroupedValueSerializer() {
                  @Override
                  public int calcTypeSize(GroupedValueAccessor accessor, long index) {
                    return Integer.BYTES + accessor.getBinaryValues().get(index).getLength();
                  }

                  @Override
                  public void serialize(
                      GroupedValueAccessor accessor, long index, byte[] bytes, int offset) {
                    accessor.getBinaryValues().toBytes(index, bytes, offset);
                  }
                };
            case BOOLEAN ->
                new GroupedValueSerializer() {
                  @Override
                  public int calcTypeSize(GroupedValueAccessor accessor, long index) {
                    return Byte.BYTES;
                  }

                  @Override
                  public void serialize(
                      GroupedValueAccessor accessor, long index, byte[] bytes, int offset) {
                    BytesUtils.boolToBytes(accessor.getBooleanValues().get(index), bytes, offset);
                  }
                };
            case ROW, UNKNOWN, VECTOR -> unsupportedGroupedValueSerializer(type);
          };

  /** Stores a column value in the typed big array selected for a grouped accumulator. */
  public static final TypeService<GroupedValueSetter> GROUPED_VALUE_SETTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (accessor, index, column, position) ->
                    accessor.getIntValues().set(index, type.getInt(column, position));
            case INT64, TIMESTAMP ->
                (accessor, index, column, position) ->
                    accessor.getLongValues().set(index, type.getLong(column, position));
            case FLOAT ->
                (accessor, index, column, position) ->
                    accessor.getFloatValues().set(index, type.getFloat(column, position));
            case DOUBLE ->
                (accessor, index, column, position) ->
                    accessor.getDoubleValues().set(index, type.getDouble(column, position));
            case TEXT, STRING, BLOB, OBJECT ->
                (accessor, index, column, position) ->
                    accessor.getBinaryValues().set(index, type.getBinary(column, position));
            case BOOLEAN ->
                (accessor, index, column, position) ->
                    accessor.getBooleanValues().set(index, type.getBoolean(column, position));
            case ROW, UNKNOWN, VECTOR ->
                (accessor, index, column, position) -> {
                  throw unsupportedDataType(type);
                };
          };

  /** Reads a grouped value from bytes into a typed callback. */
  public static final TypeService<GroupedTimeValueDeserializer>
      GROUPED_TIME_VALUE_DESERIALIZER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, DATE ->
                    (bytes, offset, groupId, time, timeNull, consumer) ->
                        consumer.updateInt(
                            groupId, BytesUtils.bytesToInt(bytes, offset), timeNull, time);
                case INT64, TIMESTAMP ->
                    (bytes, offset, groupId, time, timeNull, consumer) ->
                        consumer.updateLong(
                            groupId,
                            BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset),
                            timeNull,
                            time);
                case FLOAT ->
                    (bytes, offset, groupId, time, timeNull, consumer) ->
                        consumer.updateFloat(
                            groupId, BytesUtils.bytesToFloat(bytes, offset), timeNull, time);
                case DOUBLE ->
                    (bytes, offset, groupId, time, timeNull, consumer) ->
                        consumer.updateDouble(
                            groupId, BytesUtils.bytesToDouble(bytes, offset), timeNull, time);
                case TEXT, STRING, BLOB, OBJECT ->
                    (bytes, offset, groupId, time, timeNull, consumer) -> {
                      int length = BytesUtils.bytesToInt(bytes, offset);
                      consumer.updateBinary(
                          groupId,
                          new Binary(BytesUtils.subBytes(bytes, offset + Integer.BYTES, length)),
                          timeNull,
                          time);
                    };
                case BOOLEAN ->
                    (bytes, offset, groupId, time, timeNull, consumer) ->
                        consumer.updateBoolean(
                            groupId, BytesUtils.bytesToBool(bytes, offset), timeNull, time);
                case ROW, UNKNOWN, VECTOR ->
                    (bytes, offset, groupId, time, timeNull, consumer) -> {
                      throw unsupportedDataType(type);
                    };
              };

  /** Reads x values from FIRST_BY/LAST_BY intermediate bytes. */
  public static final TypeService<GroupedByValueDeserializer>
      GROUPED_BY_VALUE_DESERIALIZER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, DATE ->
                    (bytes, offset, groupId, xNull, time, timeNull, consumer) ->
                        consumer.updateInt(
                            groupId,
                            xNull,
                            xNull ? 0 : BytesUtils.bytesToInt(bytes, offset),
                            timeNull,
                            time);
                case INT64, TIMESTAMP ->
                    (bytes, offset, groupId, xNull, time, timeNull, consumer) ->
                        consumer.updateLong(
                            groupId,
                            xNull,
                            xNull ? 0 : BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset),
                            timeNull,
                            time);
                case FLOAT ->
                    (bytes, offset, groupId, xNull, time, timeNull, consumer) ->
                        consumer.updateFloat(
                            groupId,
                            xNull,
                            xNull ? 0 : BytesUtils.bytesToFloat(bytes, offset),
                            timeNull,
                            time);
                case DOUBLE ->
                    (bytes, offset, groupId, xNull, time, timeNull, consumer) ->
                        consumer.updateDouble(
                            groupId,
                            xNull,
                            xNull ? 0 : BytesUtils.bytesToDouble(bytes, offset),
                            timeNull,
                            time);
                case TEXT, STRING, BLOB, OBJECT ->
                    (bytes, offset, groupId, xNull, time, timeNull, consumer) -> {
                      Binary value = null;
                      if (!xNull) {
                        int length = BytesUtils.bytesToInt(bytes, offset);
                        value =
                            new Binary(BytesUtils.subBytes(bytes, offset + Integer.BYTES, length));
                      }
                      consumer.updateBinary(groupId, xNull, value, timeNull, time);
                    };
                case BOOLEAN ->
                    (bytes, offset, groupId, xNull, time, timeNull, consumer) ->
                        consumer.updateBoolean(
                            groupId,
                            xNull,
                            xNull ? false : BytesUtils.bytesToBool(bytes, offset),
                            timeNull,
                            time);
                case ROW, UNKNOWN, VECTOR ->
                    (bytes, offset, groupId, xNull, time, timeNull, consumer) -> {
                      throw unsupportedDataType(type);
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

  /** Selects the y-column reader used by grouped MAX_BY and MIN_BY input processing. */
  public static final TypeService<GroupedMaxMinByInput> GROUPED_MAX_MIN_BY_INPUT_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (groupIds, arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateInt(
                                groupIds[position],
                                type.getInt(arguments[1], position),
                                arguments[0],
                                position));
            case INT64, TIMESTAMP ->
                (groupIds, arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateLong(
                                groupIds[position],
                                type.getLong(arguments[1], position),
                                arguments[0],
                                position));
            case FLOAT ->
                (groupIds, arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateFloat(
                                groupIds[position],
                                type.getFloat(arguments[1], position),
                                arguments[0],
                                position));
            case DOUBLE ->
                (groupIds, arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateDouble(
                                groupIds[position],
                                type.getDouble(arguments[1], position),
                                arguments[0],
                                position));
            case TEXT, STRING, BLOB, OBJECT ->
                (groupIds, arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateBinary(
                                groupIds[position],
                                type.getBinary(arguments[1], position),
                                arguments[0],
                                position));
            case BOOLEAN ->
                (groupIds, arguments, mask, updater) ->
                    forEachSelectedPosition(
                        arguments[1],
                        mask,
                        position ->
                            updater.updateBoolean(
                                groupIds[position],
                                type.getBoolean(arguments[1], position),
                                arguments[0],
                                position));
            case ROW, UNKNOWN, VECTOR ->
                (groupIds, arguments, mask, updater) -> {
                  throw unsupportedMaxMinByDataType(type);
                };
          };

  /** Selects the y-value decoder used by grouped MAX_BY and MIN_BY intermediate input. */
  public static final TypeService<GroupedMaxMinByIntermediateInput>
      GROUPED_MAX_MIN_BY_INTERMEDIATE_INPUT_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case INT32, DATE ->
                    (groupId, bytes, xType, columnBuilder, updater) -> {
                      int value = BytesUtils.bytesToInt(bytes, 0);
                      readMaxMinByXFromBytes(bytes, Integer.BYTES, xType, columnBuilder);
                      updater.updateInt(groupId, value, columnBuilder.build(), 0);
                    };
                case INT64, TIMESTAMP ->
                    (groupId, bytes, xType, columnBuilder, updater) -> {
                      long value = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
                      readMaxMinByXFromBytes(bytes, Long.BYTES, xType, columnBuilder);
                      updater.updateLong(groupId, value, columnBuilder.build(), 0);
                    };
                case FLOAT ->
                    (groupId, bytes, xType, columnBuilder, updater) -> {
                      float value = BytesUtils.bytesToFloat(bytes, 0);
                      readMaxMinByXFromBytes(bytes, Float.BYTES, xType, columnBuilder);
                      updater.updateFloat(groupId, value, columnBuilder.build(), 0);
                    };
                case DOUBLE ->
                    (groupId, bytes, xType, columnBuilder, updater) -> {
                      double value = BytesUtils.bytesToDouble(bytes, 0);
                      readMaxMinByXFromBytes(bytes, Double.BYTES, xType, columnBuilder);
                      updater.updateDouble(groupId, value, columnBuilder.build(), 0);
                    };
                case TEXT, STRING, BLOB, OBJECT ->
                    (groupId, bytes, xType, columnBuilder, updater) -> {
                      int length = BytesUtils.bytesToInt(bytes, 0);
                      int xOffset = Integer.BYTES + length;
                      Binary value = new Binary(BytesUtils.subBytes(bytes, Integer.BYTES, length));
                      readMaxMinByXFromBytes(bytes, xOffset, xType, columnBuilder);
                      updater.updateBinary(groupId, value, columnBuilder.build(), 0);
                    };
                case BOOLEAN ->
                    (groupId, bytes, xType, columnBuilder, updater) -> {
                      boolean value = BytesUtils.bytesToBool(bytes, 0);
                      readMaxMinByXFromBytes(bytes, Byte.BYTES, xType, columnBuilder);
                      updater.updateBoolean(groupId, value, columnBuilder.build(), 0);
                    };
                case ROW, UNKNOWN, VECTOR ->
                    (groupId, bytes, xType, columnBuilder, updater) -> {
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
    VALUE_FILL_SERVICE.check();
    JOIN_KEY_COMPARATOR_SERVICE.check();
    GREATEST_COLUMN_TRANSFORMER_SERVICE.check();
    LEAST_COLUMN_TRANSFORMER_SERVICE.check();
    IN_COLUMN_VALUE_MATCHER_SERVICE.check();
    IN_MULTI_COLUMN_TRANSFORMER_SERVICE.check();
    UPDATE_LAST_ROW_SERVICE.check();
    PRIMITIVE_COLUMN_VALUE_SETTER_SERVICE.check();
    STATISTICS_VALUE_SETTER_SERVICE.check();
    MIN_STATISTICS_VALUE_UPDATER_SERVICE.check();
    MAX_STATISTICS_VALUE_UPDATER_SERVICE.check();
    EXTREME_STATISTICS_VALUE_UPDATER_SERVICE.check();
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
    CAST_INPUT_SERVICE.check();
    CAST_VALUE_SERVICE.check();
    CAST_OBJECT_VALUE_SERVICE.check();
    CAST_OBJECT_INPUT_SERVICE.check();
    FORMAT_VALUE_CONVERTER_SERVICE.check();
    GROUPED_APPROX_MOST_FREQUENT_ACCUMULATOR_SERVICE.check();
    APPROX_MOST_FREQUENT_ACCUMULATOR_SERVICE.check();
    APPROX_PERCENTILE_INPUT_SERVICE.check();
    GROUPED_APPROX_PERCENTILE_INPUT_SERVICE.check();
    GROUPED_VALUE_SERVICE.check();
    GROUPED_INPUT_READER_SERVICE.check();
    GROUPED_EXTREME_VALUE_SERVICE.check();
    GROUPED_EXTREME_INPUT_READER_SERVICE.check();
    GROUPED_TIME_INPUT_READER_SERVICE.check();
    GROUPED_BY_INPUT_READER_SERVICE.check();
    GROUPED_VALUE_SERIALIZER_SERVICE.check();
    GROUPED_VALUE_SETTER_SERVICE.check();
    GROUPED_TIME_VALUE_DESERIALIZER_SERVICE.check();
    GROUPED_BY_VALUE_DESERIALIZER_SERVICE.check();
    GROUPED_MAX_MIN_BY_INPUT_SERVICE.check();
    GROUPED_MAX_MIN_BY_INTERMEDIATE_INPUT_SERVICE.check();
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

  private static UnsupportedOperationException unsupportedCastTarget(final Type type) {
    return new UnsupportedOperationException(
        String.format("Unsupported target dataType: %s", type.getTypeEnum()));
  }

  private static UnsupportedOperationException unsupportedCastSource(final Type type) {
    return new UnsupportedOperationException(
        String.format("Unsupported source dataType: %s", type.getTypeEnum()));
  }

  private static UnSupportedDataTypeException unsupportedApproxMostFrequentDataType(
      final Type type) {
    return new UnSupportedDataTypeException(
            String.format(
                CalcMessages.UNSUPPORTED_DATA_TYPE_IN_APPROX_COUNT_DISTINCT_AGGREGATION,
                type.getTypeEnum()))
        .setChecked(true);
  }

  private static GroupedValueService unsupportedGroupedValueService(final Type type) {
    return new GroupedValueService() {
      @Override
      public void initialize(GroupedValueAccessor accessor) {
        throw accessor.unsupportedException();
      }

      @Override
      public void ensureCapacity(GroupedValueAccessor accessor, long groupCount) {
        throw accessor.unsupportedException();
      }

      @Override
      public long sizeOf(GroupedValueAccessor accessor) {
        throw accessor.unsupportedException();
      }

      @Override
      public void reset(GroupedValueAccessor accessor) {
        throw accessor.unsupportedException();
      }

      @Override
      public void write(GroupedValueAccessor accessor, int groupId, ColumnBuilder columnBuilder) {
        throw accessor.unsupportedException();
      }
    };
  }

  private static GroupedInputReader unsupportedGroupedInputReader(final Type type) {
    return new GroupedInputReader() {
      @Override
      public void addInput(
          int[] groupIds, Column[] arguments, AggregationMask mask, GroupedInputConsumer consumer) {
        throw unsupportedDataType(type);
      }

      @Override
      public void addIntermediate(int[] groupIds, Column argument, GroupedInputConsumer consumer) {
        throw unsupportedDataType(type);
      }
    };
  }

  private static GroupedValueSerializer unsupportedGroupedValueSerializer(final Type type) {
    return new GroupedValueSerializer() {
      @Override
      public int calcTypeSize(GroupedValueAccessor accessor, long index) {
        throw unsupportedDataType(type);
      }

      @Override
      public void serialize(GroupedValueAccessor accessor, long index, byte[] bytes, int offset) {
        throw unsupportedDataType(type);
      }
    };
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

  private static Binary toBinary(Object value) {
    return value instanceof Binary
        ? (Binary) value
        : new Binary(String.valueOf(value), java.nio.charset.StandardCharsets.UTF_8);
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
  public interface InColumnValueMatcherFactory {
    InColumnValueMatcher create(Set<String> values);
  }

  @FunctionalInterface
  public interface InColumnValueMatcher {
    boolean matches(Column column, int position);
  }

  @FunctionalInterface
  public interface InMultiColumnTransformerFactory {
    InMultiColumnTransformer create(
        List<ColumnTransformer> valueColumnTransformers, List<Literal> values);
  }

  @FunctionalInterface
  public interface CastInputService {
    void cast(
        Column column,
        ColumnBuilder columnBuilder,
        int position,
        CastValueService castValueService,
        ZoneId zoneId);
  }

  public interface CastValueService {
    void castInt(ColumnBuilder columnBuilder, int value);

    void castDate(ColumnBuilder columnBuilder, int value, ZoneId zoneId);

    void castTimestamp(ColumnBuilder columnBuilder, long value, ZoneId zoneId);

    void castLong(ColumnBuilder columnBuilder, long value);

    void castFloat(ColumnBuilder columnBuilder, float value);

    void castDouble(ColumnBuilder columnBuilder, double value);

    void castBoolean(ColumnBuilder columnBuilder, boolean value);

    void castString(ColumnBuilder columnBuilder, Binary value, ZoneId zoneId);

    void castBlob(ColumnBuilder columnBuilder, Binary value, ZoneId zoneId);

    void castObject(ColumnBuilder columnBuilder, Binary value);
  }

  public interface CastObjectValueService {
    Object castInt(int value);

    Object castDate(int value, ZoneId zoneId);

    Object castTimestamp(long value, ZoneId zoneId);

    Object castLong(long value);

    Object castFloat(float value);

    Object castDouble(double value);

    Object castBoolean(boolean value);

    Object castBinary(Binary value, ZoneId zoneId);
  }

  @FunctionalInterface
  public interface CastObjectInputService {
    Object cast(Object value, CastObjectValueService castValueService, ZoneId zoneId);
  }

  @FunctionalInterface
  public interface FormatValueConverter {
    Object convert(Column column, int position, ZoneId zoneId);
  }

  @FunctionalInterface
  public interface PrimitiveColumnValueSetter {
    void set(TsPrimitiveType target, Column column, int position);
  }

  @FunctionalInterface
  public interface StatisticsValueSetter {
    void set(
        TsPrimitiveType target,
        Object value,
        Supplier<? extends RuntimeException> exceptionSupplier);
  }

  @FunctionalInterface
  public interface StatisticsValueUpdater {
    boolean update(
        TsPrimitiveType result,
        Object value,
        boolean initialized,
        Supplier<? extends RuntimeException> exceptionSupplier);
  }

  @FunctionalInterface
  public interface TriConsumer<A, B, C> {
    void accept(A first, B second, C third);
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

  public interface GroupedValueAccessor {
    RuntimeException unsupportedException();

    void initializeIntValues();

    void initializeLongValues();

    void initializeFloatValues();

    void initializeDoubleValues();

    void initializeBinaryValues();

    void initializeBooleanValues();

    IntBigArray getIntValues();

    LongBigArray getLongValues();

    FloatBigArray getFloatValues();

    DoubleBigArray getDoubleValues();

    BinaryBigArray getBinaryValues();

    BooleanBigArray getBooleanValues();
  }

  public interface GroupedValueService {
    void initialize(GroupedValueAccessor accessor);

    void ensureCapacity(GroupedValueAccessor accessor, long groupCount);

    long sizeOf(GroupedValueAccessor accessor);

    void reset(GroupedValueAccessor accessor);

    void write(GroupedValueAccessor accessor, int groupId, ColumnBuilder columnBuilder);
  }

  public interface GroupedInputConsumer {
    void updateInt(int groupId, int value);

    void updateLong(int groupId, long value);

    void updateFloat(int groupId, float value);

    void updateDouble(int groupId, double value);

    void updateBinary(int groupId, Binary value);

    void updateBoolean(int groupId, boolean value);
  }

  public interface GroupedInputReader {
    void addInput(
        int[] groupIds, Column[] arguments, AggregationMask mask, GroupedInputConsumer consumer);

    void addIntermediate(int[] groupIds, Column argument, GroupedInputConsumer consumer);
  }

  public interface GroupedTimeInputReader {
    void addInput(
        int[] groupIds,
        Column[] arguments,
        AggregationMask mask,
        GroupedTimeValueConsumer consumer);
  }

  public interface GroupedTimeValueConsumer {
    void updateInt(int groupId, int value, boolean timeNull, long time);

    void updateLong(int groupId, long value, boolean timeNull, long time);

    void updateFloat(int groupId, float value, boolean timeNull, long time);

    void updateDouble(int groupId, double value, boolean timeNull, long time);

    void updateBinary(int groupId, Binary value, boolean timeNull, long time);

    void updateBoolean(int groupId, boolean value, boolean timeNull, long time);
  }

  public interface GroupedByInputReader {
    void addInput(
        int[] groupIds, Column[] arguments, AggregationMask mask, GroupedByValueConsumer consumer);
  }

  public interface GroupedByValueConsumer {
    void updateInt(int groupId, boolean xNull, int value, boolean timeNull, long time);

    void updateLong(int groupId, boolean xNull, long value, boolean timeNull, long time);

    void updateFloat(int groupId, boolean xNull, float value, boolean timeNull, long time);

    void updateDouble(int groupId, boolean xNull, double value, boolean timeNull, long time);

    void updateBinary(int groupId, boolean xNull, Binary value, boolean timeNull, long time);

    void updateBoolean(int groupId, boolean xNull, boolean value, boolean timeNull, long time);
  }

  public interface GroupedValueSerializer {
    int calcTypeSize(GroupedValueAccessor accessor, long index);

    void serialize(GroupedValueAccessor accessor, long index, byte[] bytes, int offset);
  }

  @FunctionalInterface
  public interface GroupedValueSetter {
    void set(GroupedValueAccessor accessor, long index, Column column, int position);
  }

  @FunctionalInterface
  public interface GroupedTimeValueDeserializer {
    void deserialize(
        byte[] bytes,
        int offset,
        int groupId,
        long time,
        boolean timeNull,
        GroupedTimeValueConsumer consumer);
  }

  @FunctionalInterface
  public interface GroupedByValueDeserializer {
    void deserialize(
        byte[] bytes,
        int offset,
        int groupId,
        boolean xNull,
        long time,
        boolean timeNull,
        GroupedByValueConsumer consumer);
  }

  public interface GroupedMaxMinByValueUpdater {
    void updateInt(int groupId, int value, Column xColumn, int xIndex);

    void updateLong(int groupId, long value, Column xColumn, int xIndex);

    void updateFloat(int groupId, float value, Column xColumn, int xIndex);

    void updateDouble(int groupId, double value, Column xColumn, int xIndex);

    void updateBinary(int groupId, Binary value, Column xColumn, int xIndex);

    void updateBoolean(int groupId, boolean value, Column xColumn, int xIndex);
  }

  @FunctionalInterface
  public interface GroupedMaxMinByInput {
    void add(
        int[] groupIds,
        Column[] arguments,
        AggregationMask mask,
        GroupedMaxMinByValueUpdater updater);
  }

  @FunctionalInterface
  public interface GroupedMaxMinByIntermediateInput {
    void update(
        int groupId,
        byte[] bytes,
        Type xType,
        ColumnBuilder columnBuilder,
        GroupedMaxMinByValueUpdater updater);
  }

  @FunctionalInterface
  public interface ValueFillFactory {
    IFill create(Literal literal, ZoneId zoneId);
  }
}
