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
import org.apache.iotdb.db.queryengine.transformation.dag.util.InputRowUtils;
import org.apache.iotdb.db.queryengine.transformation.datastructure.Cache;
import org.apache.iotdb.db.queryengine.transformation.datastructure.SerializableList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.iterator.RowListForwardIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An elastic list of records that implements memory control using LRU strategy. */
public class ElasticSerializableRowList {
  protected static final int MEMORY_CHECK_THRESHOLD = 1000;

  protected TSDataType[] dataTypes;
  protected String queryId;
  protected float memoryLimitInMB;
  protected int internalRowRecordListCapacity;
  protected int numCacheBlock;

  protected LRUCache cache;
  protected List<SerializableRowList> internalRowList;
  /** Mark bitMaps of correct index when one row has at least one null field. */
  protected List<BitMap> bitMaps;

  protected List<Integer> blockCount;

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
    internalRowRecordListCapacity = allocatableCapacity / numCacheBlock;
    if (internalRowRecordListCapacity == 0) {
      numCacheBlock = 1;
      internalRowRecordListCapacity = allocatableCapacity;
    }
    this.numCacheBlock = numCacheBlock;

    cache = new ElasticSerializableRowList.LRUCache(numCacheBlock);
    internalRowList = new ArrayList<>();
    bitMaps = new ArrayList<>();
    blockCount = new ArrayList<>();

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
      int internalRowRecordListCapacity,
      int numCacheBlock) {
    this.dataTypes = dataTypes;
    this.queryId = queryId;
    this.memoryLimitInMB = memoryLimitInMB;
    this.internalRowRecordListCapacity = internalRowRecordListCapacity;
    this.numCacheBlock = numCacheBlock;

    cache = new ElasticSerializableRowList.LRUCache(numCacheBlock);
    internalRowList = new ArrayList<>();
    bitMaps = new ArrayList<>();
    blockCount = new ArrayList<>();

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
    return internalRowRecordListCapacity;
  }

  public int getSerializableRowListSize() {
    return internalRowList.size();
  }

  public SerializableRowList getSerializableRowList(int index) throws IOException {
    return cache.get(index);
  }

  // region single row methods
  @TestOnly
  public void put(Object[] rowRecord) throws IOException, QueryProcessException {
    put(rowRecord, InputRowUtils.hasNullField(rowRecord));
  }

  /**
   * Put the row in the list with an any-field-null marker, this method is faster than calling put
   * directly.
   *
   * @throws IOException by checkMemoryUsage()
   * @throws QueryProcessException by checkMemoryUsage()
   */
  private void put(Object[] rowRecord, boolean hasNullField)
      throws IOException, QueryProcessException {
    checkExpansion();
    cache.get(rowCount / internalRowRecordListCapacity).putRow(rowRecord);
    if (hasNullField) {
      bitMaps
          .get(rowCount / internalRowRecordListCapacity)
          .mark(rowCount % internalRowRecordListCapacity);
    }
    ++rowCount;

    if (!disableMemoryControl) {
      totalByteArrayLengthLimit += rowByteArrayLength;

      if (rowRecord == null) {
        totalByteArrayLength += rowByteArrayLength;
      } else {
        for (int indexListOfTextField : indexListOfTextFields) {
          Binary binary = (Binary) rowRecord[indexListOfTextField];
          totalByteArrayLength += binary == null ? 0 : binary.getLength();
        }
        checkMemoryUsage();
      }
    }
  }

  public long getTime(int index) throws IOException {
    return cache
        .get(index / internalRowRecordListCapacity)
        .getTime(index % internalRowRecordListCapacity);
  }

  public Object[] getRowRecord(int index) throws IOException {
    return cache
        .get(index / internalRowRecordListCapacity)
        .getRow(index % internalRowRecordListCapacity);
  }
  // endregion

  // region batch rows methods
  public Column[] getColumns(int externalIndex, int internalIndex) throws IOException {
    return cache.get(externalIndex).getColumns(internalIndex);
  }

  public void put(Column[] columns) throws IOException, QueryProcessException {
    checkExpansion();

    int begin = 0, end = 0;
    int total = columns[0].getPositionCount();
    while (total > 0) {
      int consumed;
      Column[] insertedColumns;
      if (total + rowCount % internalRowRecordListCapacity < internalRowRecordListCapacity) {
        consumed = total;
        insertedColumns = columns;
      } else {
        consumed = internalRowRecordListCapacity - rowCount % internalRowRecordListCapacity;
        // Construct sub-regions
        insertedColumns = new Column[columns.length];
        for (int i = 0; i < columns.length; i++) {
          insertedColumns[i] = columns[i].copyRegion(begin, consumed);
        }
      }

      end += consumed;

      // Fill row record list and bitmap
      cache.get(rowCount / internalRowRecordListCapacity).putColumns(insertedColumns);
      markBitMapByColumns(insertedColumns, begin, end);

      total -= consumed;
      rowCount += consumed;
      begin = end;

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
  // endregion

  public RowListForwardIterator constructIterator() {
    RowListForwardIterator iterator = new RowListForwardIterator(this);
    iteratorList.add(iterator);

    return iterator;
  }

  public RowListForwardIterator constructIteratorByEvictionUpperBound() throws IOException {
    int externalColumnIndex = evictionUpperBound / internalRowRecordListCapacity;

    int internalPointIndex = evictionUpperBound % internalRowRecordListCapacity;
    int internalColumnIndex =
        internalRowList.get(externalColumnIndex).getColumnIndex(internalPointIndex);

    // This iterator is for memory control.
    // So there is no need to put it into iterator list since it won't be affected by new memory
    // control strategy.
    return new RowListForwardIterator(this, externalColumnIndex, internalColumnIndex);
  }

  private void checkExpansion() {
    if (rowCount % internalRowRecordListCapacity == 0) {
      doExpansion();
    }
  }

  private void doExpansion() {
    if (internalRowList.size() > 1) {
      // Add last row record list's block count
      blockCount.add(internalRowList.get(internalRowList.size() - 1).getBlockCount());
    }

    internalRowList.add(SerializableRowList.construct(queryId, dataTypes));
    bitMaps.add(new BitMap(internalRowRecordListCapacity));
  }

  private void markBitMapByColumns(Column[] columns, int from, int to) {
    BitMap bitmap = bitMaps.get(rowCount / internalRowRecordListCapacity);

    int offset = rowCount % internalRowRecordListCapacity;
    for (int i = from; i < to; i++) {
      for (Column column : columns) {
        if (column.isNull(i)) {
          bitmap.mark(offset + i - from);
          break;
        }
      }
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
      newESRowList.bitMaps.add(null);
    }
    // Copy latter lists
    newESRowList.rowCount = internalListEvictionUpperBound * newInternalRowRecordListCapacity;
    RowListForwardIterator iterator = constructIteratorByEvictionUpperBound();
    copyColumnByIterator(newESRowList, iterator);
    while (iterator.hasNext()) {
      iterator.next();
      copyColumnByIterator(newESRowList, iterator);
    }

    internalRowRecordListCapacity = newInternalRowRecordListCapacity;
    cache = newESRowList.cache;
    internalRowList = newESRowList.internalRowList;
    bitMaps = newESRowList.bitMaps;

    byteArrayLengthForMemoryControl = newByteArrayLengthForMemoryControl;
    rowByteArrayLength = (long) byteArrayLengthForMemoryControl * indexListOfTextFields.length;
    totalByteArrayLengthLimit = (long) rowCount * rowByteArrayLength;

    notifyAllIterators();
  }

  public void notifyAllIterators() throws IOException {
    for (RowListForwardIterator iterator : iteratorList) {
      iterator.adjust();
    }
  }

  private void copyColumnByIterator(
      ElasticSerializableRowList target, RowListForwardIterator source)
      throws IOException, QueryProcessException {
    Column[] columns = source.currentBlock();
    target.put(columns);
  }

  /** true if any field except the timestamp in the current row is null. */
  @TestOnly
  public boolean fieldsHasAnyNull(int index) {
    return bitMaps
        .get(index / internalRowRecordListCapacity)
        .isMarked(index % internalRowRecordListCapacity);
  }

  public int getFirstRowIndex(int externalIndex, int internalIndex) throws IOException {
    int index = internalRowRecordListCapacity * externalIndex;
    int offset = cache.get(externalIndex).getFirstRowIndex(internalIndex);

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
          if (lastIndex < evictionUpperBound / internalRowRecordListCapacity) {
            internalRowList.set(lastIndex, null);
            bitMaps.set(lastIndex, null);
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
