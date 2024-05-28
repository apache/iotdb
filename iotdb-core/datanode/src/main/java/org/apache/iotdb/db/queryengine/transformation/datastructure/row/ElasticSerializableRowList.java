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

package org.apache.iotdb.db.queryengine.transformation.datastructure.row;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.datastructure.Cache;
import org.apache.iotdb.db.queryengine.transformation.datastructure.SerializableList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.iterator.RowListForwardIterator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An elastic list of records that implements memory control using LRU strategy. */
public class ElasticSerializableRowList {
  protected static final int MEMORY_CHECK_THRESHOLD = 1000;

  protected TSDataType[] dataTypes;
  protected String queryId;
  protected float memoryLimitInMB;
  protected int internalRowListCapacity;
  protected int numCacheBlock;

  protected LRUCache cache;
  protected List<SerializableRowList> internalRowList;
  protected List<Integer> internalBlockCountList;

  protected int rowCount;
  protected int lastRowCount;
  protected int evictionUpperBound;

  protected boolean disableMemoryControl;
  protected int[] indexListOfTextFields;
  protected int byteArrayLengthForMemoryControl;
  protected long rowByteArrayLength;
  protected long totalByteArrayLengthLimit;
  protected long totalByteArrayLength;

  protected List<RowListForwardIterator> iteratorList;

  /**
   * Construct a ElasticSerializableRowRecordList.
   *
   * @param dataTypes Data types of columns.
   * @param queryId Query ID.
   * @param memoryLimitInMB Memory limit.
   * @param numCacheBlock Number of cache blocks.
   * @throws QueryProcessException by SerializableRowRecordList.calculateCapacity
   */
  public ElasticSerializableRowList(
      TSDataType[] dataTypes, String queryId, float memoryLimitInMB, int numCacheBlock)
      throws QueryProcessException {
    this.dataTypes = dataTypes;
    this.queryId = queryId;
    this.memoryLimitInMB = memoryLimitInMB;
    int allocatableCapacity =
        SerializableRowList.calculateCapacity(
            dataTypes,
            memoryLimitInMB,
            SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL);
    internalRowListCapacity = allocatableCapacity / numCacheBlock;
    if (internalRowListCapacity == 0) {
      numCacheBlock = 1;
      internalRowListCapacity = allocatableCapacity;
    }
    this.numCacheBlock = numCacheBlock;

    cache = new ElasticSerializableRowList.LRUCache(numCacheBlock);
    internalRowList = new ArrayList<>();
    internalBlockCountList = new ArrayList<>();

    rowCount = 0;
    evictionUpperBound = 0;

    disableMemoryControl = true;
    int textFieldsCount = 0;
    for (TSDataType dataType : dataTypes) {
      if (dataType.equals(TSDataType.TEXT)) {
        ++textFieldsCount;
        disableMemoryControl = false;
      }
    }
    indexListOfTextFields = new int[textFieldsCount];
    int fieldIndex = 0;
    for (int i = 0; i < dataTypes.length; ++i) {
      if (dataTypes[i].equals(TSDataType.TEXT)) {
        indexListOfTextFields[fieldIndex++] = i;
      }
    }
    byteArrayLengthForMemoryControl = SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
    totalByteArrayLengthLimit = 0;
    totalByteArrayLength = 0;
    rowByteArrayLength = (long) byteArrayLengthForMemoryControl * textFieldsCount;

    iteratorList = new ArrayList<>();
  }

  protected ElasticSerializableRowList(
      TSDataType[] dataTypes,
      String queryId,
      float memoryLimitInMB,
      int internalRowListCapacity,
      int numCacheBlock) {
    this.dataTypes = dataTypes;
    this.queryId = queryId;
    this.memoryLimitInMB = memoryLimitInMB;
    this.internalRowListCapacity = internalRowListCapacity;
    this.numCacheBlock = numCacheBlock;

    cache = new ElasticSerializableRowList.LRUCache(numCacheBlock);
    internalRowList = new ArrayList<>();
    internalBlockCountList = new ArrayList<>();

    rowCount = 0;
    evictionUpperBound = 0;

    disableMemoryControl = true;

    iteratorList = new ArrayList<>();
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public int size() {
    return rowCount;
  }

  public int getInternalRowListCapacity() {
    return internalRowListCapacity;
  }

  public int getSerializableRowListSize() {
    return internalRowList.size();
  }

  public SerializableRowList getSerializableRowList(int index) {
    // Do not cache this row list
    return internalRowList.get(index);
  }

  // region single row methods for row window
  public long getTime(int index) throws IOException {
    return cache.get(index / internalRowListCapacity).getTime(index % internalRowListCapacity);
  }

  public Object[] getRowRecord(int index) throws IOException {
    return cache.get(index / internalRowListCapacity).getRow(index % internalRowListCapacity);
  }

  // endregion

  // region batch rows methods
  public Column[] getColumns(int externalIndex, int internalIndex) throws IOException {
    return cache.get(externalIndex).getColumns(internalIndex);
  }

  public void put(Column[] columns) throws IOException, QueryProcessException {
    // Check if we need to add new internal list
    checkExpansion();

    int begin = 0, end = 0;
    int total = columns[0].getPositionCount();
    while (total > 0) {
      int consumed;
      Column[] insertedColumns;
      if (total + rowCount % internalRowListCapacity < internalRowListCapacity) {
        consumed = total;
        if (begin == 0) {
          // No need to copy if the columns do not split
          insertedColumns = columns;
        } else {
          insertedColumns = new Column[columns.length];
          for (int i = 0; i < columns.length; i++) {
            insertedColumns[i] = columns[i].getRegionCopy(begin, consumed);
          }
        }
      } else {
        consumed = internalRowListCapacity - rowCount % internalRowListCapacity;
        // Construct sub-regions
        insertedColumns = new Column[columns.length];
        for (int i = 0; i < columns.length; i++) {
          insertedColumns[i] = columns[i].getRegionCopy(begin, consumed);
        }
      }

      end += consumed;
      begin = end;
      total -= consumed;

      // Fill row record list
      cache.get(rowCount / internalRowListCapacity).putColumns(insertedColumns);
      rowCount += consumed;
      if (total > 0) {
        doExpansion();
      }
    }

    if (!disableMemoryControl) {
      int count = columns[0].getPositionCount();
      totalByteArrayLengthLimit += rowByteArrayLength * count;

      for (int i = 0; i < count; i++) {
        for (int indexListOfTextField : indexListOfTextFields) {
          Column bianryColumn = columns[indexListOfTextField];
          if (!bianryColumn.isNull(i)) {
            totalByteArrayLength += bianryColumn.getBinary(i).getLength();
          }
        }
      }

      checkMemoryUsage();
    }
  }

  public void putNulls(int total) throws IOException {
    // Only batch insert nulls at the beginning of internal list
    assert rowCount % internalRowListCapacity == 0;

    // Check if we need to add new internal list
    checkExpansion();
    while (total > 0) {
      int consumed = Math.min(total, internalRowListCapacity);
      cache.get(rowCount / internalRowListCapacity).putNulls(consumed);

      total -= consumed;
      rowCount += consumed;
      if (total > 0) {
        doExpansion();
      }
    }

    if (!disableMemoryControl) {
      totalByteArrayLengthLimit += rowByteArrayLength * total;
      totalByteArrayLength += rowByteArrayLength * total;
    }
  }

  // endregion

  public RowListForwardIterator constructIterator() {
    RowListForwardIterator iterator = new RowListForwardIterator(this);
    iteratorList.add(iterator);

    return iterator;
  }

  private void copyLatterColumnsAfterEvictionUpperBound(ElasticSerializableRowList newESRowList)
      throws IOException, QueryProcessException {
    int externalColumnIndex = evictionUpperBound / internalRowListCapacity;

    int internalRowIndex = evictionUpperBound % internalRowListCapacity;
    int internalColumnIndex =
        internalRowList.get(externalColumnIndex).getColumnIndex(internalRowIndex);
    int rowOffsetInColumns =
        internalRowList.get(externalColumnIndex).getRowOffsetInColumns(internalRowIndex);

    // This iterator is for memory control.
    // So there is no need to put it into iterator list since it won't be affected by new memory
    // control strategy.
    RowListForwardIterator iterator =
        new RowListForwardIterator(this, externalColumnIndex, internalColumnIndex);
    // Get and put split columns after eviction upper bound
    Column[] columns = iterator.currentBlock();
    if (rowOffsetInColumns != 0) {
      for (int i = 0; i < columns.length; i++) {
        columns[i] = columns[i].subColumnCopy(rowOffsetInColumns);
      }
    }
    newESRowList.put(columns);

    // Copy latter columns
    while (iterator.hasNext()) {
      iterator.next();
      copyColumnByIterator(newESRowList, iterator);
    }
  }

  private void copyColumnByIterator(
      ElasticSerializableRowList target, RowListForwardIterator source)
      throws IOException, QueryProcessException {
    Column[] columns = source.currentBlock();
    target.put(columns);
  }

  private void checkExpansion() {
    if (rowCount % internalRowListCapacity == 0) {
      doExpansion();
    }
  }

  private void doExpansion() {
    if (internalRowList.size() > 0) {
      int lastIndex = internalRowList.size() - 1;
      SerializableRowList lastInternalList = internalRowList.get(lastIndex);
      // For put nulls
      if (lastInternalList != null) {
        internalBlockCountList.add(lastInternalList.getBlockCount());
      } else {
        internalBlockCountList.add(0);
      }
    }
    internalRowList.add(SerializableRowList.construct(queryId, dataTypes));
  }

  public int getBlockCount(int index) {
    if (index == internalRowList.size() - 1) {
      SerializableRowList lastList = internalRowList.get(index);
      return lastList.getBlockCount();
    } else {
      return internalBlockCountList.get(index);
    }
  }

  protected void checkMemoryUsage() throws IOException, QueryProcessException {
    // Insert more than MEMORY_CHECK_THRESHOLD rows
    // and actual memory footprint is greater than expected
    // to apply new memory control strategy
    if (rowCount - lastRowCount < MEMORY_CHECK_THRESHOLD
        || totalByteArrayLength <= totalByteArrayLengthLimit) {
      return;
    }

    // Exponential growth theoretical byte array length
    lastRowCount = rowCount;
    int newByteArrayLengthForMemoryControl = byteArrayLengthForMemoryControl;
    while ((long) newByteArrayLengthForMemoryControl * rowCount < totalByteArrayLength) {
      newByteArrayLengthForMemoryControl *= 2;
    }
    int newInternalTVListCapacity =
        SerializableRowList.calculateCapacity(
                dataTypes, memoryLimitInMB, newByteArrayLengthForMemoryControl)
            / numCacheBlock;
    if (newInternalTVListCapacity > 0) {
      applyNewMemoryControlParameters(
          newByteArrayLengthForMemoryControl, newInternalTVListCapacity);
      return;
    }

    // Try to find a more suitable parameters
    int delta =
        (int)
            ((totalByteArrayLength - totalByteArrayLengthLimit)
                / rowCount
                / indexListOfTextFields.length
                / SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL);
    newByteArrayLengthForMemoryControl =
        byteArrayLengthForMemoryControl
            + 2 * (delta + 1) * SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
    newInternalTVListCapacity =
        SerializableRowList.calculateCapacity(
                dataTypes, memoryLimitInMB, newByteArrayLengthForMemoryControl)
            / numCacheBlock;
    if (newInternalTVListCapacity > 0) {
      applyNewMemoryControlParameters(
          newByteArrayLengthForMemoryControl, newInternalTVListCapacity);
      return;
    }

    throw new QueryProcessException("Memory is not enough for current query.");
  }

  protected void applyNewMemoryControlParameters(
      int newByteArrayLengthForMemoryControl, int newInternalRowRecordListCapacity)
      throws IOException, QueryProcessException {
    ElasticSerializableRowList newESRowList =
        new ElasticSerializableRowList(
            dataTypes, queryId, memoryLimitInMB, newInternalRowRecordListCapacity, numCacheBlock);

    // Set previous evicted list to null
    newESRowList.evictionUpperBound = evictionUpperBound;
    int internalListEvictionUpperBound = evictionUpperBound / newInternalRowRecordListCapacity;
    for (int i = 0; i < internalListEvictionUpperBound; ++i) {
      newESRowList.internalRowList.add(null);
      if (i != 0) {
        newESRowList.internalBlockCountList.add(0);
      }
    }
    // Put all null columns to middle list
    newESRowList.rowCount = internalListEvictionUpperBound * newInternalRowRecordListCapacity;
    int emptyColumnSize = evictionUpperBound - newESRowList.rowCount;
    if (emptyColumnSize != 0) {
      newESRowList.putNulls(emptyColumnSize);
    }
    // Copy latter columns
    copyLatterColumnsAfterEvictionUpperBound(newESRowList);

    // Replace old list with new ones
    internalRowListCapacity = newInternalRowRecordListCapacity;
    cache = newESRowList.cache;
    internalRowList = newESRowList.internalRowList;
    internalBlockCountList = newESRowList.internalBlockCountList;
    // Update metrics
    byteArrayLengthForMemoryControl = newByteArrayLengthForMemoryControl;
    rowByteArrayLength = (long) byteArrayLengthForMemoryControl * indexListOfTextFields.length;
    totalByteArrayLengthLimit = (long) rowCount * rowByteArrayLength;

    // Notify all iterators to update
    notifyAllIterators();
  }

  public void notifyAllIterators() throws IOException {
    for (RowListForwardIterator iterator : iteratorList) {
      iterator.adjust();
    }
  }

  /** true if any field except the timestamp in the current row is null. */
  @TestOnly
  public boolean fieldsHasAnyNull(int index) throws IOException {
    Object[] row =
        internalRowList
            .get(index / internalRowListCapacity)
            .getRow(index % internalRowListCapacity);
    for (Object field : row) {
      if (field == null) {
        return true;
      }
    }

    return false;
  }

  public int getLastRowIndex(int externalIndex, int internalIndex) {
    int index = internalRowListCapacity * externalIndex;
    int offset = internalRowList.get(externalIndex).getLastRowIndex(internalIndex);

    return index + offset;
  }

  /**
   * Set the upper bound.
   *
   * @param evictionUpperBound the index of the first element that cannot be evicted. in other
   *     words, elements whose index are <b>less than</b> the evictionUpperBound can be evicted.
   */
  public void setEvictionUpperBound(int evictionUpperBound) {
    this.evictionUpperBound = evictionUpperBound;
  }

  private class LRUCache extends Cache {

    LRUCache(int capacity) {
      super(capacity);
    }

    SerializableRowList get(int targetIndex) throws IOException {
      if (!containsKey(targetIndex)) {
        if (cacheCapacity <= super.size()) {
          int lastIndex = getLast();
          if (lastIndex < evictionUpperBound / internalRowListCapacity) {
            internalRowList.set(lastIndex, null);
          } else {
            internalRowList.get(lastIndex).serialize();
          }
        }
        internalRowList.get(targetIndex).deserialize();
      }
      putKey(targetIndex);
      return internalRowList.get(targetIndex);
    }
  }
}
