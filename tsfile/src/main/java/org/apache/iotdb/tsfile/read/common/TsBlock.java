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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import static java.lang.String.format;

/**
 * Intermediate result for most of ExecOperators. The Tablet contains data from one or more columns
 * and constructs them as a row based view The columns can be series, aggregation result for one
 * series or scalar value (such as deviceName). The Tablet also contains the metadata to describe
 * the columns.
 *
 * <p>TODO: consider the detailed data store model in memory. (using column based or row based ?)
 */
public class TsBlock {

  // Describe the column info
  private TsBlockMetadata metadata;

  private TimeColumn timeColumn;

  private Column[] valueColumns;

  private int count;

  public TsBlock() {}

  public TsBlock(int columnCount) {
    timeColumn = new TimeColumn();
    valueColumns = new Column[columnCount];
  }

  public boolean hasNext() {
    return false;
  }

  public void next() {}

  public TsBlockMetadata getMetadata() {
    return metadata;
  }

  public int getCount() {
    return count;
  }

  /** TODO need to be implemented after the data structure being defined */
  public long getEndTime() {
    return -1;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  /**
   * TODO has not been implemented yet
   *
   * @param positionOffset start offset
   * @param length slice length
   * @return view of current TsBlock start from positionOffset to positionOffset + length
   */
  public TsBlock getRegion(int positionOffset, int length) {
    if (positionOffset < 0 || length < 0 || positionOffset + length > count) {
      throw new IndexOutOfBoundsException(
          format(
              "Invalid position %s and length %s in page with %s positions",
              positionOffset, length, count));
    }
    return this;
  }

  /** TODO need to be implemented after the data structure being defined */
  public long getTimeByIndex(int index) {
    return -1;
  }

  public void addTime(long time) {}

  public int getValueColumnCount() {
    return valueColumns.length;
  }

  public TimeColumn getTimeColumn() {
    return timeColumn;
  }

  public Column getColumn(int columnIndex) {
    return valueColumns[columnIndex];
  }

  public int addValues(
      int columnIndex, TimeColumn timeColumn, Column valueColumn, int rowIndex, long endTime) {

    return rowIndex;
  }

  public TsBlockIterator getTsBlockIterator() {
    return new TsBlockIterator();
  }

  // TODO need to be implemented when the data structure is defined
  private class TsBlockIterator implements IPointReader, IBatchDataIterator {

    @Override
    public boolean hasNext() {
      return TsBlock.this.hasNext();
    }

    @Override
    public boolean hasNext(long minBound, long maxBound) {
      return hasNext();
    }

    @Override
    public void next() {
      TsBlock.this.next();
    }

    @Override
    public long currentTime() {
      return -1;
    }

    @Override
    public Object currentValue() {
      return null;
    }

    @Override
    public void reset() {}

    @Override
    public int totalLength() {
      return TsBlock.this.getCount();
    }

    @Override
    public boolean hasNextTimeValuePair() {
      return hasNext();
    }

    @Override
    public TimeValuePair nextTimeValuePair() {
      return null;
    }

    @Override
    public TimeValuePair currentTimeValuePair() {
      return null;
    }

    @Override
    public void close() {}
  }
}
