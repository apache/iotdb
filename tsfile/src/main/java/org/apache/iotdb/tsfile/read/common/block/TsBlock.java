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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read.common.block;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Iterator;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Intermediate result for most of ExecOperators. The TsBlock contains data from one or more columns
 * and constructs them as a row based view The columns can be series, aggregation result for one
 * series or scalar value (such as deviceName).
 */
public class TsBlock {

  public static final int INSTANCE_SIZE = ClassLayout.parseClass(TsBlock.class).instanceSize();

  private static final Column[] EMPTY_COLUMNS = new Column[0];

  /**
   * Visible to give trusted classes like {@link TsBlockBuilder} access to a constructor that
   * doesn't defensively copy the valueColumns
   */
  public static TsBlock wrapBlocksWithoutCopy(
      int positionCount, TimeColumn timeColumn, Column[] valueColumns) {
    return new TsBlock(false, positionCount, timeColumn, valueColumns);
  }

  private final TimeColumn timeColumn;

  private final Column[] valueColumns;

  /** How many rows in current TsBlock */
  private final int positionCount;

  private volatile long retainedSizeInBytes = -1;

  public TsBlock(int positionCount) {
    this(false, positionCount, null, EMPTY_COLUMNS);
  }

  public TsBlock(TimeColumn timeColumn, Column... valueColumns) {
    this(true, determinePositionCount(valueColumns), timeColumn, valueColumns);
  }

  public TsBlock(int positionCount, TimeColumn timeColumn, Column... valueColumns) {
    this(true, positionCount, timeColumn, valueColumns);
  }

  private TsBlock(
      boolean columnsCopyRequired,
      int positionCount,
      TimeColumn timeColumn,
      Column[] valueColumns) {
    requireNonNull(valueColumns, "blocks is null");
    this.positionCount = positionCount;
    this.timeColumn = timeColumn;
    if (valueColumns.length == 0) {
      this.valueColumns = EMPTY_COLUMNS;
      // Empty blocks are not considered "retained" by any particular page
      this.retainedSizeInBytes = INSTANCE_SIZE;
    } else {
      this.valueColumns = columnsCopyRequired ? valueColumns.clone() : valueColumns;
    }
  }

  public int getPositionCount() {
    return positionCount;
  }

  public long getStartTime() {
    return timeColumn.getStartTime();
  }

  public long getEndTime() {
    return timeColumn.getEndTime();
  }

  public boolean isEmpty() {
    return positionCount == 0;
  }

  public long getRetainedSizeInBytes() {
    long retainedSizeInBytes = this.retainedSizeInBytes;
    if (retainedSizeInBytes < 0) {
      return updateRetainedSize();
    }
    return retainedSizeInBytes;
  }

  /**
   * @param positionOffset start offset
   * @param length slice length
   * @return view of current TsBlock start from positionOffset to positionOffset + length
   */
  public TsBlock getRegion(int positionOffset, int length) {
    if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
      throw new IndexOutOfBoundsException(
          format(
              "Invalid position %s and length %s in page with %s positions",
              positionOffset, length, positionCount));
    }
    int channelCount = getValueColumnCount();
    Column[] slicedColumns = new Column[channelCount];
    for (int i = 0; i < channelCount; i++) {
      slicedColumns[i] = valueColumns[i].getRegion(positionOffset, length);
    }
    return wrapBlocksWithoutCopy(
        length, (TimeColumn) timeColumn.getRegion(positionOffset, length), slicedColumns);
  }

  public TsBlock appendValueColumn(Column column) {
    requireNonNull(column, "Column is null");
    if (positionCount != column.getPositionCount()) {
      throw new IllegalArgumentException("Block does not have same position count");
    }

    Column[] newBlocks = Arrays.copyOf(valueColumns, valueColumns.length + 1);
    newBlocks[valueColumns.length] = column;
    return wrapBlocksWithoutCopy(positionCount, timeColumn, newBlocks);
  }

  /**
   * Attention. This method uses System.arraycopy() to extend the valueColumn array, so its
   * performance is not ensured if you have many insert operations.
   */
  public TsBlock insertValueColumn(int index, Column column) {
    requireNonNull(column, "Column is null");
    if (positionCount != column.getPositionCount()) {
      throw new IllegalArgumentException("Block does not have same position count");
    }

    Column[] newBlocks = Arrays.copyOf(valueColumns, valueColumns.length + 1);
    System.arraycopy(newBlocks, index, newBlocks, index + 1, valueColumns.length - index);
    newBlocks[index] = column;
    return wrapBlocksWithoutCopy(positionCount, timeColumn, newBlocks);
  }

  /**
   * This method will create a temporary view of origin tsBlock, which will reuse the arrays of
   * columns but with different offset. It can be used where you want to skip some points when
   * getting iterator.
   */
  public TsBlock subTsBlock(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("FromIndex of subTsBlock cannot over positionCount.");
    }
    TimeColumn subTimeColumn = (TimeColumn) timeColumn.subColumn(fromIndex);
    Column[] subValueColumns = new Column[valueColumns.length];
    for (int i = 0; i < subValueColumns.length; i++) {
      subValueColumns[i] = valueColumns[i].subColumn(fromIndex);
    }
    return new TsBlock(subTimeColumn, subValueColumns);
  }

  public TsBlock skipFirst() {
    return this.subTsBlock(1);
  }

  public long getTimeByIndex(int index) {
    return timeColumn.getLong(index);
  }

  public int getValueColumnCount() {
    return valueColumns.length;
  }

  public TimeColumn getTimeColumn() {
    return timeColumn;
  }

  public Column[] getValueColumns() {
    return valueColumns;
  }

  public Column getColumn(int columnIndex) {
    return valueColumns[columnIndex];
  }

  public Column[] getTimeAndValueColumn(int columnIndex) {
    Column[] columns = new Column[2];
    columns[0] = getTimeColumn();
    columns[1] = getColumn(columnIndex);
    return columns;
  }

  public Column[] getColumns(int[] columnIndexes) {
    Column[] columns = new Column[columnIndexes.length];
    for (int i = 0; i < columnIndexes.length; i++) {
      columns[i] = valueColumns[columnIndexes[i]];
    }
    return columns;
  }

  public TsBlockSingleColumnIterator getTsBlockSingleColumnIterator() {
    return new TsBlockSingleColumnIterator(0);
  }

  public TsBlockSingleColumnIterator getTsBlockSingleColumnIterator(int columnIndex) {
    return new TsBlockSingleColumnIterator(0, columnIndex);
  }

  public void reverse() {
    timeColumn.reverse();
    for (Column valueColumn : valueColumns) {
      valueColumn.reverse();
    }
  }

  public TsBlockRowIterator getTsBlockRowIterator() {
    return new TsBlockRowIterator(0);
  }

  /** Only used for the batch data of vector time series. */
  public TsBlockAlignedRowIterator getTsBlockAlignedRowIterator() {
    return new TsBlockAlignedRowIterator(0);
  }

  public class TsBlockSingleColumnIterator implements IPointReader, IBatchDataIterator {

    private int rowIndex;
    private final int columnIndex;

    public TsBlockSingleColumnIterator(int rowIndex) {
      this.rowIndex = rowIndex;
      this.columnIndex = 0;
    }

    public TsBlockSingleColumnIterator(int rowIndex, int columnIndex) {
      this.rowIndex = rowIndex;
      this.columnIndex = columnIndex;
    }

    @Override
    public boolean hasNext() {
      return rowIndex < positionCount;
    }

    @Override
    public boolean hasNext(long minBound, long maxBound) {
      return hasNext();
    }

    @Override
    public void next() {
      rowIndex++;
    }

    @Override
    public long currentTime() {
      return timeColumn.getLong(rowIndex);
    }

    @Override
    public Object currentValue() {
      return valueColumns[columnIndex].getTsPrimitiveType(rowIndex).getValue();
    }

    @Override
    public void reset() {
      rowIndex = 0;
    }

    @Override
    public int totalLength() {
      return positionCount;
    }

    @Override
    public boolean hasNextTimeValuePair() {
      return hasNext();
    }

    @Override
    public TimeValuePair nextTimeValuePair() {
      TimeValuePair res = currentTimeValuePair();
      next();
      return res;
    }

    @Override
    public TimeValuePair currentTimeValuePair() {
      return new TimeValuePair(
          timeColumn.getLong(rowIndex), valueColumns[columnIndex].getTsPrimitiveType(rowIndex));
    }

    @Override
    public void close() {}

    public long getEndTime() {
      return TsBlock.this.getEndTime();
    }

    public long getStartTime() {
      return TsBlock.this.getStartTime();
    }

    public int getRowIndex() {
      return rowIndex;
    }

    public void setRowIndex(int rowIndex) {
      this.rowIndex = rowIndex;
    }
  }

  /** Mainly used for UDF framework. Note that the timestamps are at the last column. */
  public class TsBlockRowIterator implements Iterator<Object[]> {

    protected int rowIndex;
    protected int columnCount;

    public TsBlockRowIterator(int rowIndex) {
      this.rowIndex = rowIndex;
      columnCount = getValueColumnCount();
    }

    @Override
    public boolean hasNext() {
      return rowIndex < positionCount;
    }

    /** @return A row in the TsBlock. The timestamp is at the last column. */
    @Override
    public Object[] next() {
      int columnCount = getValueColumnCount();
      Object[] row = new Object[columnCount + 1];
      for (int i = 0; i < columnCount; ++i) {
        final Column column = valueColumns[i];
        row[i] = column.isNull(rowIndex) ? null : column.getObject(rowIndex);
      }
      row[columnCount] = timeColumn.getObject(rowIndex);

      rowIndex++;

      return row;
    }
  }

  private class TsBlockAlignedRowIterator implements IPointReader, IBatchDataIterator {

    private int rowIndex;

    public TsBlockAlignedRowIterator(int rowIndex) {
      this.rowIndex = rowIndex;
    }

    @Override
    public boolean hasNext() {
      return rowIndex < positionCount;
    }

    @Override
    public boolean hasNext(long minBound, long maxBound) {
      while (hasNext()) {
        if (currentTime() < minBound || currentTime() >= maxBound) {
          break;
        }
        next();
      }
      return hasNext();
    }

    @Override
    public void next() {
      rowIndex++;
    }

    @Override
    public long currentTime() {
      return timeColumn.getLong(rowIndex);
    }

    @Override
    public TsPrimitiveType[] currentValue() {
      TsPrimitiveType[] tsPrimitiveTypes = new TsPrimitiveType[valueColumns.length];
      for (int i = 0; i < valueColumns.length; i++) {
        if (!valueColumns[i].isNull(rowIndex)) {
          tsPrimitiveTypes[i] = valueColumns[i].getTsPrimitiveType(rowIndex);
        }
      }
      return tsPrimitiveTypes;
    }

    @Override
    public void reset() {
      rowIndex = 0;
    }

    @Override
    public int totalLength() {
      return positionCount;
    }

    @Override
    public boolean hasNextTimeValuePair() {
      while (hasNext() && isCurrentValueAllNull()) {
        next();
      }
      return hasNext();
    }

    @Override
    public TimeValuePair nextTimeValuePair() {
      TimeValuePair res = currentTimeValuePair();
      next();
      return res;
    }

    @Override
    public TimeValuePair currentTimeValuePair() {
      return new TimeValuePair(
          timeColumn.getLong(rowIndex), new TsPrimitiveType.TsVector(currentValue()));
    }

    @Override
    public void close() {}

    public long getEndTime() {
      return TsBlock.this.getEndTime();
    }

    public long getStartTime() {
      return TsBlock.this.getStartTime();
    }

    public int getRowIndex() {
      return rowIndex;
    }

    public void setRowIndex(int rowIndex) {
      this.rowIndex = rowIndex;
    }

    private boolean isCurrentValueAllNull() {
      for (int i = 0; i < valueColumns.length; i++) {
        if (!valueColumns[i].isNull(rowIndex)) {
          return false;
        }
      }
      return true;
    }
  }

  private long updateRetainedSize() {
    long retainedSizeInBytes = INSTANCE_SIZE;
    retainedSizeInBytes += timeColumn.getRetainedSizeInBytes();
    for (Column column : valueColumns) {
      retainedSizeInBytes += column.getRetainedSizeInBytes();
    }
    this.retainedSizeInBytes = retainedSizeInBytes;
    return retainedSizeInBytes;
  }

  public int getTotalInstanceSize() {
    int totalInstanceSize = INSTANCE_SIZE;
    totalInstanceSize += timeColumn.getInstanceSize();
    for (Column column : valueColumns) {
      totalInstanceSize += column.getInstanceSize();
    }
    return totalInstanceSize;
  }

  private static int determinePositionCount(Column... columns) {
    requireNonNull(columns, "columns is null");
    if (columns.length == 0) {
      throw new IllegalArgumentException("columns is empty");
    }

    return columns[0].getPositionCount();
  }
}
