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

package org.apache.iotdb.db.queryengine.transformation.datastructure.tv;

import org.apache.iotdb.db.queryengine.transformation.datastructure.SerializableList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.iterator.TVListForwardIterator;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.BinaryUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import java.io.IOException;

public class ElasticSerializableBinaryTVList extends ElasticSerializableTVList {
  protected static final int MEMORY_CHECK_THRESHOLD = 1000;

  protected int byteArrayLengthForMemoryControl;

  protected long totalByteArrayLengthLimit;
  protected long totalByteArrayLength;

  protected int lastPointCount;

  public ElasticSerializableBinaryTVList(String queryId, float memoryLimitInMB, int cacheSize) {
    super(TSDataType.TEXT, queryId, memoryLimitInMB, cacheSize);
    byteArrayLengthForMemoryControl = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
    totalByteArrayLengthLimit = 0;
    totalByteArrayLength = 0;
  }

  @Override
  public void putColumn(Column timeColumn, Column valueColumn) throws IOException {
    super.putColumn(timeColumn, valueColumn);

    // Update two memory control statistics
    long count = timeColumn.getPositionCount();
    totalByteArrayLengthLimit += byteArrayLengthForMemoryControl * count;
    for (int i = 0; i < count; i++) {
      if (valueColumn.isNull(i)) {
        totalByteArrayLength += byteArrayLengthForMemoryControl;
      } else {
        totalByteArrayLength += valueColumn.getBinary(i).getLength();
      }
    }

    checkMemoryUsage();
  }

  private void checkMemoryUsage() throws IOException {
    // Insert more than MEMORY_CHECK_THRESHOLD points
    // and actual memory footprint is greater than expected
    // to apply new memory control strategy
    if (pointCount - lastPointCount < MEMORY_CHECK_THRESHOLD
        || totalByteArrayLength <= totalByteArrayLengthLimit) {
      return;
    }

    // Exponential growth theoretical byte array length
    lastPointCount = pointCount;
    int newByteArrayLengthForMemoryControl = byteArrayLengthForMemoryControl;
    while ((long) newByteArrayLengthForMemoryControl * pointCount < totalByteArrayLength) {
      newByteArrayLengthForMemoryControl *= 2;
    }
    int newInternalTVListCapacity =
        BinaryUtils.calculateCapacity(memoryLimitInMB, newByteArrayLengthForMemoryControl)
            / cacheSize;
    if (newInternalTVListCapacity > 0) {
      applyNewMemoryControlParameters(
          newByteArrayLengthForMemoryControl, newInternalTVListCapacity);
      return;
    }

    // Try to find a more suitable parameters
    int delta =
        (int)
            ((totalByteArrayLength - totalByteArrayLengthLimit)
                / pointCount
                / SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL);
    newByteArrayLengthForMemoryControl =
        byteArrayLengthForMemoryControl
            + 2 * (delta + 1) * SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
    newInternalTVListCapacity =
        BinaryUtils.calculateCapacity(memoryLimitInMB, newByteArrayLengthForMemoryControl)
            / cacheSize;
    if (newInternalTVListCapacity > 0) {
      applyNewMemoryControlParameters(
          newByteArrayLengthForMemoryControl, newInternalTVListCapacity);
      return;
    }

    throw new RuntimeException("Memory is not enough for current query.");
  }

  private void applyNewMemoryControlParameters(
      int newByteArrayLengthForMemoryControl, int newInternalTVListCapacity) throws IOException {
    ElasticSerializableTVList newESTVList =
        new ElasticSerializableTVList(
            TSDataType.TEXT, queryId, memoryLimitInMB, newInternalTVListCapacity, cacheSize);

    // Set previous evicted list to null
    newESTVList.evictionUpperBound = evictionUpperBound;
    int internalListEvictionUpperBound = evictionUpperBound / newInternalTVListCapacity;
    for (int i = 0; i < internalListEvictionUpperBound; ++i) {
      newESTVList.internalTVList.add(null);
      if (i != 0) {
        newESTVList.internalColumnCountList.add(0);
      }
    }
    // Put all null columns to middle list
    newESTVList.pointCount = internalListEvictionUpperBound * newInternalTVListCapacity;
    int emptyColumnSize = evictionUpperBound - newESTVList.pointCount;
    if (emptyColumnSize != 0) {
      Binary empty = BytesUtils.valueOf("");
      TimeColumnBuilder timeColumnBuilder = new TimeColumnBuilder(null, emptyColumnSize);
      ColumnBuilder valueColumnBuilder = new BinaryColumnBuilder(null, emptyColumnSize);
      for (int i = 0; i < emptyColumnSize; i++) {
        timeColumnBuilder.writeLong(i);
        valueColumnBuilder.writeBinary(empty);
      }
      TimeColumn timeColumn = (TimeColumn) timeColumnBuilder.build();
      Column valueColumn = valueColumnBuilder.build();
      newESTVList.putColumn(timeColumn, valueColumn);
    }
    // Copy latter lists
    copyLatterColumnsAfterEvictionUpperBound(newESTVList);

    // Assign new tvList to the old
    internalTVListCapacity = newInternalTVListCapacity;
    cache = newESTVList.cache;
    internalTVList = newESTVList.internalTVList;
    internalColumnCountList = newESTVList.internalColumnCountList;
    // Update metrics
    byteArrayLengthForMemoryControl = newByteArrayLengthForMemoryControl;
    totalByteArrayLengthLimit = (long) pointCount * byteArrayLengthForMemoryControl;

    // Notify all iterators to update
    notifyAllIterators();
  }

  public void copyLatterColumnsAfterEvictionUpperBound(ElasticSerializableTVList newESTVList)
      throws IOException {
    int externalColumnIndex = evictionUpperBound / internalTVListCapacity;

    int internalRowIndex = evictionUpperBound % internalTVListCapacity;
    int internalColumnIndex =
        internalTVList.get(externalColumnIndex).getColumnIndex(internalRowIndex);
    int tvOffsetInColumns =
        internalTVList.get(externalColumnIndex).getTVOffsetInColumns(internalRowIndex);

    // This iterator is for memory control.
    // So there is no need to put it into iterator list since it won't be affected by new memory
    // control strategy.
    TVListForwardIterator iterator =
        new TVListForwardIterator(this, externalColumnIndex, internalColumnIndex);
    // Get and put split columns after eviction upper bound
    Column timeColumn = iterator.currentTimes();
    Column valueColumn = iterator.currentValues();
    if (tvOffsetInColumns != 0) {
      timeColumn = timeColumn.subColumnCopy(tvOffsetInColumns);
      valueColumn = valueColumn.subColumnCopy(tvOffsetInColumns);
    }
    newESTVList.putColumn(timeColumn, valueColumn);

    // Copy latter columns
    while (iterator.hasNext()) {
      iterator.next();
      copyColumnByIterator(newESTVList, iterator);
    }
  }

  public void notifyAllIterators() throws IOException {
    for (TVListForwardIterator iterator : iteratorList) {
      iterator.adjust();
    }
  }

  private void copyColumnByIterator(ElasticSerializableTVList target, TVListForwardIterator source)
      throws IOException {
    Column timeColumn = source.currentTimes();
    Column valueColumn = source.currentValues();
    target.putColumn(timeColumn, valueColumn);
  }
}
