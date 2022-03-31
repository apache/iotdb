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

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Intermediate result for most of ExecOperators. The Tablet contains data from one or more columns
 * and constructs them as a row based view The columns can be series, aggregation result for one
 * series or scalar value (such as deviceName). The Tablet also contains the metadata to describe
 * the columns.
 */
public class TsBlock {

  public static final int INSTANCE_SIZE = ClassLayout.parseClass(TsBlock.class).instanceSize();

  private static final Column[] EMPTY_COLUMNS = new Column[0];

  /**
   * Visible to give trusted classes like {@link TsBlockBuilder} access to a constructor that
   * doesn't defensively copy the valueColumns
   */
  static TsBlock wrapBlocksWithoutCopy(
      int positionCount, TimeColumn timeColumn, Column[] valueColumns) {
    return new TsBlock(false, positionCount, timeColumn, valueColumns);
  }

  // TODO rethink about if we really need this field
  // Describe the column info
  private TsBlockMetadata metadata;

  private final TimeColumn timeColumn;

  private final Column[] valueColumns;

  private final int positionCount;

  private volatile long retainedSizeInBytes = -1;

  public TsBlock(TimeColumn timeColumn, Column... valueColumns) {
    this(true, determinePositionCount(valueColumns), timeColumn, valueColumns);
  }

  public TsBlock(int positionCount) {
    this(false, positionCount, null, EMPTY_COLUMNS);
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

  public boolean hasNext() {
    return false;
  }

  public void next() {}

  public TsBlockMetadata getMetadata() {
    return metadata;
  }

  public int getPositionCount() {
    return positionCount;
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
    requireNonNull(column, "column is null");
    if (positionCount != column.getPositionCount()) {
      throw new IllegalArgumentException("Block does not have same position count");
    }

    Column[] newBlocks = Arrays.copyOf(valueColumns, valueColumns.length + 1);
    newBlocks[valueColumns.length] = column;
    return wrapBlocksWithoutCopy(positionCount, timeColumn, newBlocks);
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

  public Column getColumn(int columnIndex) {
    return valueColumns[columnIndex];
  }

  public TsBlockIterator getTsBlockIterator() {
    return new TsBlockIterator(0);
  }

  /** Only used for the batch data of vector time series. */
  public IBatchDataIterator getTsBlockIterator(int subIndex) {
    return new AlignedTsBlockIterator(0, subIndex);
  }

  private class TsBlockIterator implements IPointReader, IBatchDataIterator {

    protected int index;

    public TsBlockIterator(int index) {
      this.index = index;
    }

    @Override
    public boolean hasNext() {
      return index < positionCount;
    }

    @Override
    public boolean hasNext(long minBound, long maxBound) {
      return hasNext();
    }

    @Override
    public void next() {
      index++;
    }

    @Override
    public long currentTime() {
      return timeColumn.getLong(index);
    }

    @Override
    public Object currentValue() {
      return valueColumns[0].getTsPrimitiveType(index).getValue();
    }

    @Override
    public void reset() {
      index = 0;
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
          timeColumn.getLong(index), valueColumns[0].getTsPrimitiveType(index));
    }

    @Override
    public void close() {}
  }

  private class AlignedTsBlockIterator extends TsBlockIterator {

    private final int subIndex;

    private AlignedTsBlockIterator(int index, int subIndex) {
      super(index);
      this.subIndex = subIndex;
    }

    @Override
    public boolean hasNext() {
      while (super.hasNext() && currentValue() == null) {
        super.next();
      }
      return super.hasNext();
    }

    @Override
    public boolean hasNext(long minBound, long maxBound) {
      while (super.hasNext() && currentValue() == null) {
        if (currentTime() < minBound || currentTime() >= maxBound) {
          break;
        }
        super.next();
      }
      return super.hasNext();
    }

    @Override
    public Object currentValue() {
      TsPrimitiveType v = valueColumns[subIndex].getTsPrimitiveType(index);
      return v == null ? null : v.getValue();
    }

    @Override
    public int totalLength() {
      // aligned timeseries' BatchData length() may return the length of time column
      // we need traverse to VectorBatchDataIterator calculate the actual value column's length
      int cnt = 0;
      int indexSave = index;
      while (hasNext()) {
        cnt++;
        next();
      }
      index = indexSave;
      return cnt;
    }
  }

  private long updateRetainedSize() {
    long retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueColumns);
    retainedSizeInBytes += timeColumn.getRetainedSizeInBytes();
    for (Column column : valueColumns) {
      retainedSizeInBytes += column.getRetainedSizeInBytes();
    }
    this.retainedSizeInBytes = retainedSizeInBytes;
    return retainedSizeInBytes;
  }

  private static int determinePositionCount(Column... columns) {
    requireNonNull(columns, "columns is null");
    if (columns.length == 0) {
      throw new IllegalArgumentException("columns is empty");
    }

    return columns[0].getPositionCount();
  }
}
