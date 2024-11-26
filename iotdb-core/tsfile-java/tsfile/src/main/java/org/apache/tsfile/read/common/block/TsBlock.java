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

package org.apache.tsfile.read.common.block;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.IBatchDataIterator;
import org.apache.tsfile.read.common.block.column.ColumnFactory;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Intermediate result for most of ExecOperators. The TsBlock contains data from one or more columns
 * and constructs them as a row based view The columns can be series, aggregation result for one
 * series or scalar value (such as deviceName).
 */
public class TsBlock {

  public static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(TsBlock.class);

  private static final Column[] EMPTY_COLUMNS = new Column[0];

  /**
   * Visible to give trusted classes like {@link TsBlockBuilder} access to a constructor that
   * doesn't defensively copy the valueColumns
   */
  public static TsBlock wrapBlocksWithoutCopy(
      int positionCount, Column timeColumn, Column[] valueColumns) {
    return new TsBlock(false, positionCount, timeColumn, valueColumns);
  }

  private final Column timeColumn;

  private final Column[] valueColumns;

  /** How many rows in current TsBlock */
  private int positionCount;

  private volatile long retainedSizeInBytes = -1;

  public TsBlock(int positionCount) {
    this(false, positionCount, null, EMPTY_COLUMNS);
  }

  public TsBlock(Column timeColumn, Column... valueColumns) {
    this(true, determinePositionCount(timeColumn), timeColumn, valueColumns);
  }

  public TsBlock(int positionCount, Column timeColumn, Column... valueColumns) {
    this(true, positionCount, timeColumn, valueColumns);
  }

  private TsBlock(
      boolean columnsCopyRequired, int positionCount, Column timeColumn, Column[] valueColumns) {
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

  public void setPositionCount(int positionCount) {
    this.positionCount = positionCount;
  }

  public long getStartTime() {
    return timeColumn.getLong(0);
  }

  public long getEndTime() {
    return timeColumn.getLong(positionCount - 1);
  }

  public boolean isEmpty() {
    return positionCount == 0;
  }

  public long getRetainedSizeInBytes() {
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
        length, timeColumn.getRegion(positionOffset, length), slicedColumns);
  }

  /**
   * ATTENTION!!! The following two methods use System.arraycopy() to extend the valueColumn array,
   * so its performance is not ensured if you have many insert operations. </br> PLEASE MAKE SURE
   * the column object CAN'T BE NULL and POSITION COUNT SHOULD MATCH valueColumn's</br>
   */
  public TsBlock appendValueColumns(Column[] columns) {
    Column[] newBlocks = Arrays.copyOf(valueColumns, valueColumns.length + columns.length);
    System.arraycopy(columns, 0, newBlocks, valueColumns.length, columns.length);
    return wrapBlocksWithoutCopy(positionCount, timeColumn, newBlocks);
  }

  public TsBlock insertValueColumn(int index, Column[] columns) {
    Column[] newBlocks = Arrays.copyOf(valueColumns, valueColumns.length + columns.length);
    System.arraycopy(
        newBlocks, index, newBlocks, index + columns.length, valueColumns.length - index);
    System.arraycopy(columns, 0, newBlocks, index, columns.length);
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
    Column subTimeColumn = timeColumn.subColumn(fromIndex);
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

  public Column getTimeColumn() {
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

  /**
   * Collected all columns into a column array. Note that the timestamps are at the last column.
   *
   * @return column array composed of all columns.
   */
  public Column[] getAllColumns() {
    Column[] columns = Arrays.copyOf(valueColumns, valueColumns.length + 1);
    columns[valueColumns.length] = timeColumn;
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

  public void reset() {
    if (positionCount == 0) {
      return;
    }
    positionCount = 0;
    timeColumn.reset();
    for (Column valueColumn : valueColumns) {
      valueColumn.reset();
    }
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
    public long getUsedMemorySize() {
      return getRetainedSizeInBytes();
    }

    @Override
    public void close() {
      // do nothing
    }

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

    /**
     * @return A row in the TsBlock. The timestamp is at the last column.
     */
    @Override
    public Object[] next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      int curColumnCount = getValueColumnCount();
      Object[] row = new Object[curColumnCount + 1];
      for (int i = 0; i < curColumnCount; ++i) {
        final Column column = valueColumns[i];
        row[i] = column.isNull(rowIndex) ? null : column.getObject(rowIndex);
      }
      row[curColumnCount] = timeColumn.getObject(rowIndex);

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
    public long getUsedMemorySize() {
      return getRetainedSizeInBytes();
    }

    @Override
    public void close() {
      // do nothing
    }

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

  private long updateRetainedSize() {
    long newRetainedSizeInBytes = INSTANCE_SIZE;
    newRetainedSizeInBytes += timeColumn.getRetainedSizeInBytes();
    for (Column column : valueColumns) {
      newRetainedSizeInBytes += column.getRetainedSizeInBytes();
    }
    this.retainedSizeInBytes = newRetainedSizeInBytes;
    return newRetainedSizeInBytes;
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

  public void update(int updateIdx, TsBlock sourceTsBlock, int sourceIndex) {
    timeColumn.getLongs()[updateIdx] = sourceTsBlock.getTimeByIndex(sourceIndex);
    updateWithoutTimeColumn(updateIdx, sourceTsBlock, sourceIndex);
  }

  public void updateWithoutTimeColumn(int updateIdx, TsBlock sourceTsBlock, int sourceIndex) {
    for (int i = 0; i < getValueColumnCount(); i++) {
      if (sourceTsBlock.getValueColumns()[i].isNull(sourceIndex)) {
        valueColumns[i].isNull()[updateIdx] = true;
        continue;
      }
      switch (valueColumns[i].getDataType()) {
        case BOOLEAN:
          valueColumns[i].isNull()[updateIdx] = false;
          valueColumns[i].getBooleans()[updateIdx] =
              sourceTsBlock.getValueColumns()[i].getBoolean(sourceIndex);
          break;
        case INT32:
        case DATE:
          valueColumns[i].isNull()[updateIdx] = false;
          valueColumns[i].getInts()[updateIdx] =
              sourceTsBlock.getValueColumns()[i].getInt(sourceIndex);
          break;
        case INT64:
        case TIMESTAMP:
          valueColumns[i].isNull()[updateIdx] = false;
          valueColumns[i].getLongs()[updateIdx] =
              sourceTsBlock.getValueColumns()[i].getLong(sourceIndex);
          break;
        case FLOAT:
          valueColumns[i].isNull()[updateIdx] = false;
          valueColumns[i].getFloats()[updateIdx] =
              sourceTsBlock.getValueColumns()[i].getFloat(sourceIndex);
          break;
        case DOUBLE:
          valueColumns[i].isNull()[updateIdx] = false;
          valueColumns[i].getDoubles()[updateIdx] =
              sourceTsBlock.getValueColumns()[i].getDouble(sourceIndex);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          valueColumns[i].isNull()[updateIdx] = false;
          valueColumns[i].getBinaries()[updateIdx] =
              sourceTsBlock.getValueColumns()[i].getBinary(sourceIndex);
          break;
        default:
          throw new UnSupportedDataTypeException(
              "Unknown datatype: " + valueColumns[i].getDataType());
      }
    }
  }

  public static TsBlock buildTsBlock(List<String> columnNames, TableSchema schema, int blockSize) {
    Column timeColumn = new TimeColumn(blockSize);
    Column[] columns = new Column[columnNames.size()];
    for (int i = 0; i < columnNames.size(); i++) {
      final String columnName = columnNames.get(i);
      final IMeasurementSchema columnSchema = schema.findColumnSchema(columnName);
      columns[i] = ColumnFactory.create(columnSchema.getType(), blockSize);
    }
    return new TsBlock(timeColumn, columns);
  }

  /**
   * For each column, if its positionCount < this. positionCount, add nulls at the end of the
   * column.
   */
  public void fillTrailingNulls() {
    for (Column valueColumn : valueColumns) {
      if (valueColumn.getPositionCount() < this.positionCount) {
        valueColumn.setNull(valueColumn.getPositionCount(), this.positionCount);
      }
    }
  }
}
