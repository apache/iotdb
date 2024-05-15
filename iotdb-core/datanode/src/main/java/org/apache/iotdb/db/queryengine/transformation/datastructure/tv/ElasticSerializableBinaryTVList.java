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
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.BinaryUtils;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.iterator.TVListForwardIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

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
  public void putColumn(TimeColumn timeColumn, Column valueColumn) throws IOException {
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
    }
    // Copy latter lists
    newESTVList.pointCount = internalListEvictionUpperBound * newInternalTVListCapacity;
    TVListForwardIterator iterator = constructIteratorByEvictionUpperBound();
    copyColumnByIterator(newESTVList, iterator);
    while (iterator.hasNext()) {
      iterator.next();
      copyColumnByIterator(newESTVList, iterator);
    }

    // Assign new tvList to the old
    internalTVListCapacity = newInternalTVListCapacity;
    cache = newESTVList.cache;
    internalTVList = newESTVList.internalTVList;
    // Update metrics
    byteArrayLengthForMemoryControl = newByteArrayLengthForMemoryControl;
    totalByteArrayLengthLimit = (long) pointCount * byteArrayLengthForMemoryControl;

    // Update all iterators
    notifyAllIterators();
  }

  private void copyColumnByIterator(ElasticSerializableTVList target, TVListForwardIterator source)
      throws IOException {
    TimeColumn timeColumn = source.currentTimes();
    Column valueColumn = source.currentValues();
    target.putColumn(timeColumn, valueColumn);
  }
}
