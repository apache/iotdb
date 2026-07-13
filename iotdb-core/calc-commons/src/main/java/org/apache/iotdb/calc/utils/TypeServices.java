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
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.datastructure.SortKey;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.DateType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.IntType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.ObjectType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.service.IntTypeService;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Comparator;
import java.util.function.IntFunction;

public class TypeServices {

  public static final TypeService<IntFunction<Comparator<SortKey>>> COMPARATOR_SERVICE =
      type -> {
        if (type.getClass().equals(IntType.class) || type.getClass().equals(DateType.class)) {
          return index ->
              Comparator.comparingInt(
                  sortKey -> type.getInt(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
        } else if (type.getClass().equals(LongType.class)
            || type.getClass().equals(TimestampType.class)) {
          return index ->
              Comparator.comparingLong(
                  sortKey -> type.getLong(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
        } else if (type.getClass().equals(FloatType.class)) {
          return index ->
              Comparator.comparingDouble(
                  sortKey -> type.getFloat(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
        } else if (type.getClass().equals(DoubleType.class)) {
          return index ->
              Comparator.comparingDouble(
                  sortKey -> type.getDouble(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
        } else if (type.getClass().equals(BinaryType.class)
            || type.getClass().equals(BlobType.class)
            || type.getClass().equals(ObjectType.class)
            || type.getClass().equals(StringType.class)) {
          return index ->
              Comparator.comparing(
                  sortKey -> type.getBinary(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
        } else if (type.getClass().equals(BooleanType.class)) {
          return index ->
              Comparator.comparing(
                  sortKey -> type.getBoolean(sortKey.tsBlock.getColumn(index), sortKey.rowIndex));
        } else {
          throw new IllegalArgumentException(
              String.format(CalcMessages.DATA_TYPE_CANNOT_BE_ORDERED, type));
        }
      };

  public static final IntTypeService MEMORY_USAGE_OF_ONE_MERGE_SORT_KEY_SERVICE =
      type -> {
        if (type.getClass().equals(BooleanType.class)) {
          return 1;
        } else if (type.getClass().equals(IntType.class)
            || type.getClass().equals(FloatType.class)
            || type.getClass().equals(DateType.class)) {
          return 4;
        } else if (type.getClass().equals(LongType.class)
            || type.getClass().equals(DoubleType.class)
            || type.getClass().equals(TimestampType.class)) {
          return 8;
        } else if (type.getClass().equals(BinaryType.class)
            || type.getClass().equals(StringType.class)
            || type.getClass().equals(BlobType.class)
            || type.getClass().equals(ObjectType.class)) {
          return 16;
        } else {
          throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type);
        }
      };

  public static final TypeService<DefaultValueWriter> DEFAULT_VALUE_WRITER_SERVICE =
      type -> {
        if (type.getClass().equals(IntType.class) || type.getClass().equals(DateType.class)) {
          return (partition, channel, index, builder) ->
              builder.writeInt(partition.getInt(channel, index));
        } else if (type.getClass().equals(LongType.class)
            || type.getClass().equals(TimestampType.class)) {
          return (partition, channel, index, builder) ->
              builder.writeLong(partition.getLong(channel, index));
        } else if (type.getClass().equals(FloatType.class)) {
          return (partition, channel, index, builder) ->
              builder.writeFloat(partition.getFloat(channel, index));
        } else if (type.getClass().equals(DoubleType.class)) {
          return (partition, channel, index, builder) ->
              builder.writeDouble(partition.getDouble(channel, index));
        } else if (type.getClass().equals(BooleanType.class)) {
          return (partition, channel, index, builder) ->
              builder.writeBoolean(partition.getBoolean(channel, index));
        } else if (type.getClass().equals(BinaryType.class)
            || type.getClass().equals(StringType.class)
            || type.getClass().equals(BlobType.class)
            || type.getClass().equals(ObjectType.class)) {
          return (partition, channel, index, builder) ->
              builder.writeBinary(partition.getBinary(channel, index));
        } else {
          throw new UnSupportedDataTypeException(
              "Unsupported default value's data type in Lag: " + type);
        }
      };

  static {
    COMPARATOR_SERVICE.check();
    MEMORY_USAGE_OF_ONE_MERGE_SORT_KEY_SERVICE.check();
    DEFAULT_VALUE_WRITER_SERVICE.check();
  }

  private TypeServices() {
    // util class doesn't need constructor
  }

  @FunctionalInterface
  public interface DefaultValueWriter {
    void write(Partition partition, int channel, int index, ColumnBuilder builder);
  }
}
