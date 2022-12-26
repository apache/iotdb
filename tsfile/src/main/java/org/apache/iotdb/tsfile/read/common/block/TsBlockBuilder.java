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
package org.apache.iotdb.tsfile.read.common.block;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class TsBlockBuilder {

  // We choose default initial size to be 8 for TsBlockBuilder and ColumnBuilder
  // so the underlying data is larger than the object overhead, and the size is power of 2.
  //
  // This could be any other small number.
  private static final int DEFAULT_INITIAL_EXPECTED_ENTRIES = 8;

  private static final int MAX_LINE_NUMBER =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();

  private TimeColumnBuilder timeColumnBuilder;
  private ColumnBuilder[] valueColumnBuilders;
  private List<TSDataType> types;
  private TsBlockBuilderStatus tsBlockBuilderStatus;
  private int declaredPositions;

  private TsBlockBuilder() {}

  /**
   * Create a TsBlockBuilder with given types.
   *
   * <p>A TsBlockBuilder instance created with this constructor has no estimation about bytes per
   * entry, therefore it can resize frequently while appending new rows.
   *
   * <p>This constructor should only be used to get the initial TsBlockBuilder. Once the
   * TsBlockBuilder is full use reset() or createTsBlockBuilderLike() to create a new TsBlockBuilder
   * instance with its size estimated based on previous data.
   */
  public TsBlockBuilder(List<TSDataType> types) {
    this(DEFAULT_INITIAL_EXPECTED_ENTRIES, types);
  }

  public TsBlockBuilder(int initialExpectedEntries, List<TSDataType> types) {
    this(initialExpectedEntries, DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, types);
  }

  public static TsBlockBuilder createWithOnlyTimeColumn() {
    TsBlockBuilder res = new TsBlockBuilder();
    res.tsBlockBuilderStatus = new TsBlockBuilderStatus(DEFAULT_INITIAL_EXPECTED_ENTRIES);
    res.timeColumnBuilder =
        new TimeColumnBuilder(
            res.tsBlockBuilderStatus.createColumnBuilderStatus(), DEFAULT_INITIAL_EXPECTED_ENTRIES);
    return res;
  }

  public static TsBlockBuilder withMaxTsBlockSize(int maxTsBlockBytes, List<TSDataType> types) {
    return new TsBlockBuilder(DEFAULT_INITIAL_EXPECTED_ENTRIES, maxTsBlockBytes, types);
  }

  private TsBlockBuilder(int initialExpectedEntries, int maxTsBlockBytes, List<TSDataType> types) {
    this.types = requireNonNull(types, "types is null");

    tsBlockBuilderStatus = new TsBlockBuilderStatus(maxTsBlockBytes);
    timeColumnBuilder =
        new TimeColumnBuilder(
            tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
    valueColumnBuilders = new ColumnBuilder[types.size()];

    for (int i = 0; i < valueColumnBuilders.length; i++) {
      // TODO use Type interface to encapsulate createColumnBuilder to each concrete type class
      // instead of switch-case
      switch (types.get(i)) {
        case BOOLEAN:
          valueColumnBuilders[i] =
              new BooleanColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        case INT32:
          valueColumnBuilders[i] =
              new IntColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        case INT64:
          valueColumnBuilders[i] =
              new LongColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        case FLOAT:
          valueColumnBuilders[i] =
              new FloatColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        case DOUBLE:
          valueColumnBuilders[i] =
              new DoubleColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        case TEXT:
          valueColumnBuilders[i] =
              new BinaryColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        default:
          throw new IllegalArgumentException("Unknown data type: " + types.get(i));
      }
    }
  }

  private TsBlockBuilder(
      int maxTsBlockBytes,
      List<TSDataType> types,
      TimeColumnBuilder templateTimeColumnBuilder,
      ColumnBuilder[] templateValueColumnBuilders) {
    this.types = requireNonNull(types, "types is null");

    tsBlockBuilderStatus = new TsBlockBuilderStatus(maxTsBlockBytes);
    valueColumnBuilders = new ColumnBuilder[types.size()];

    checkArgument(
        templateValueColumnBuilders.length == types.size(),
        "Size of templates and types should match");
    timeColumnBuilder =
        (TimeColumnBuilder)
            templateTimeColumnBuilder.newColumnBuilderLike(
                tsBlockBuilderStatus.createColumnBuilderStatus());
    for (int i = 0; i < valueColumnBuilders.length; i++) {
      valueColumnBuilders[i] =
          templateValueColumnBuilders[i].newColumnBuilderLike(
              tsBlockBuilderStatus.createColumnBuilderStatus());
    }
  }

  public void buildValueColumnBuilders(List<TSDataType> types) {
    this.types = requireNonNull(types, "types is null");
    valueColumnBuilders = new ColumnBuilder[types.size()];
    int initialExpectedEntries = timeColumnBuilder.getPositionCount();
    for (int i = 0; i < valueColumnBuilders.length; i++) {
      // TODO use Type interface to encapsulate createColumnBuilder to each concrete type class
      // instead of switch-case
      switch (types.get(i)) {
        case BOOLEAN:
          valueColumnBuilders[i] =
              new BooleanColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        case INT32:
          valueColumnBuilders[i] =
              new IntColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        case INT64:
          valueColumnBuilders[i] =
              new LongColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        case FLOAT:
          valueColumnBuilders[i] =
              new FloatColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        case DOUBLE:
          valueColumnBuilders[i] =
              new DoubleColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        case TEXT:
          valueColumnBuilders[i] =
              new BinaryColumnBuilder(
                  tsBlockBuilderStatus.createColumnBuilderStatus(), initialExpectedEntries);
          break;
        default:
          throw new IllegalArgumentException("Unknown data type: " + types.get(i));
      }
    }
  }

  public void reset() {
    if (isEmpty()) {
      return;
    }
    tsBlockBuilderStatus =
        new TsBlockBuilderStatus(tsBlockBuilderStatus.getMaxTsBlockSizeInBytes());

    declaredPositions = 0;

    timeColumnBuilder =
        (TimeColumnBuilder)
            timeColumnBuilder.newColumnBuilderLike(
                tsBlockBuilderStatus.createColumnBuilderStatus());
    for (int i = 0; i < valueColumnBuilders.length; i++) {
      valueColumnBuilders[i] =
          valueColumnBuilders[i].newColumnBuilderLike(
              tsBlockBuilderStatus.createColumnBuilderStatus());
    }
  }

  public TsBlockBuilder newTsBlockBuilderLike() {
    return new TsBlockBuilder(
        tsBlockBuilderStatus.getMaxTsBlockSizeInBytes(),
        types,
        timeColumnBuilder,
        valueColumnBuilders);
  }

  public TimeColumnBuilder getTimeColumnBuilder() {
    return timeColumnBuilder;
  }

  public ColumnBuilder getColumnBuilder(int channel) {
    return valueColumnBuilders[channel];
  }

  public ColumnBuilder[] getValueColumnBuilders() {
    return valueColumnBuilders;
  }

  public TSDataType getType(int channel) {
    return types.get(channel);
  }

  // Indicate current row number
  public void declarePosition() {
    declaredPositions++;
  }

  public void declarePositions(int deltaPositions) {
    declaredPositions += deltaPositions;
  }

  public boolean isFull() {
    return declaredPositions >= MAX_LINE_NUMBER || tsBlockBuilderStatus.isFull();
  }

  public boolean isEmpty() {
    return declaredPositions == 0;
  }

  public int getPositionCount() {
    return declaredPositions;
  }

  public long getSizeInBytes() {
    return tsBlockBuilderStatus.getSizeInBytes();
  }

  public long getRetainedSizeInBytes() {
    // We use a foreach loop instead of streams
    // as it has much better performance.
    long retainedSizeInBytes = timeColumnBuilder.getRetainedSizeInBytes();
    for (ColumnBuilder columnBuilder : valueColumnBuilders) {
      retainedSizeInBytes += columnBuilder.getRetainedSizeInBytes();
    }
    return retainedSizeInBytes;
  }

  public TsBlock build() {
    if (valueColumnBuilders.length == 0) {
      return new TsBlock(declaredPositions);
    }
    TimeColumn timeColumn = (TimeColumn) timeColumnBuilder.build();
    if (timeColumn.getPositionCount() != declaredPositions) {
      throw new IllegalStateException(
          format(
              "Declared positions (%s) does not match time column's number of entries (%s)",
              declaredPositions, timeColumn.getPositionCount()));
    }

    Column[] columns = new Column[valueColumnBuilders.length];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = valueColumnBuilders[i].build();
      if (columns[i].getPositionCount() != declaredPositions) {
        throw new IllegalStateException(
            format(
                "Declared positions (%s) does not match column %s's number of entries (%s)",
                declaredPositions, i, columns[i].getPositionCount()));
      }
    }

    return TsBlock.wrapBlocksWithoutCopy(declaredPositions, timeColumn, columns);
  }

  /**
   * Write a text value to the columnIndex. If the value is null, then the place will be recorded
   * with null.
   *
   * @param columnIndex the target column index.
   * @param value the text value to be inserted.
   */
  public void writeNullableText(int columnIndex, String value) {
    if (value == null) {
      getColumnBuilder(columnIndex).appendNull();
    } else {
      getColumnBuilder(columnIndex).writeBinary(new Binary(value));
    }
  }

  private static void checkArgument(boolean expression, String errorMessage) {
    if (!expression) {
      throw new IllegalArgumentException(errorMessage);
    }
  }
}
