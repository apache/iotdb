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

import org.apache.iotdb.calc.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedMaxMinByBaseAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.BinaryBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.BooleanBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.FloatBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.IntBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.datastructure.SortKey;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.type.service.IntTypeService;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Comparator;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

import static org.apache.iotdb.calc.transformation.datastructure.util.BinaryUtils.MIN_ARRAY_HEADER_SIZE;
import static org.apache.iotdb.calc.transformation.datastructure.util.BinaryUtils.MIN_OBJECT_HEADER_SIZE;

public class TypeServices {

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
            default ->
                throw new IllegalArgumentException(
                    String.format(CalcMessages.DATA_TYPE_CANNOT_BE_ORDERED, type));
          };

  public static final IntTypeService MEMORY_USAGE_OF_ONE_MERGE_SORT_KEY_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> 1;
            case INT32, FLOAT, DATE -> 4;
            case INT64, DOUBLE, TIMESTAMP -> 8;
            case TEXT, STRING, BLOB, OBJECT -> 16;
            default -> throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type);
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
                default -> throw new UnSupportedDataTypeException(type.toString());
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
                default ->
                    throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type);
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
            default ->
                throw new UnSupportedDataTypeException(
                    "Unsupported default value's data type in Lag: " + type);
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
            default -> throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type);
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
                default ->
                    throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type);
              };

  static {
    MERGE_SORT_COMPARATOR_SERVICE.check();
    MEMORY_USAGE_OF_ONE_MERGE_SORT_KEY_SERVICE.check();
    MEMORY_USAGE_OF_ONE_SERIALIZABLE_ROW_FIELD_SERVICE.check();
    DEFAULT_ENCODING_BY_TYPE_SERVICE.check();
    DEFAULT_VALUE_WRITER_SERVICE.check();
    INTERMEDIATE_VALUE_WRITER_SERVICE.check();
    INTERMEDIATE_VALUE_INITIALIZER_SERVICE.check();
  }

  private TypeServices() {
    // util class doesn't need constructor
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
