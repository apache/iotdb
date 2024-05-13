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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.dag.util.InputRowUtils;
import org.apache.iotdb.db.queryengine.transformation.datastructure.Cache;
import org.apache.iotdb.db.queryengine.transformation.datastructure.SerializableList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.iterator.RowListForwardIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.RowColumnConverter.appendRowInColumnBuilders;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.RowColumnConverter.buildColumnsByBuilders;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.RowColumnConverter.constructColumnBuilders;

/** An elastic list of records that implements memory control using LRU strategy. */
public class ElasticSerializableRowRecordList {

  protected static final int MEMORY_CHECK_THRESHOLD = 1000;

  protected TSDataType[] dataTypes;
  protected String queryId;
  protected float memoryLimitInMB;
  protected int internalRowRecordListCapacity;
  protected int numCacheBlock;

  protected LRUCache cache;
  protected List<SerializableRowRecordList> rowLists;
  /** Mark bitMaps of correct index when one row has at least one null field. */
  protected List<BitMap> bitMaps;

  protected List<Integer> blockCount;

  protected int rowCount;
  protected int evictionUpperBound;

  protected boolean disableMemoryControl;
  protected int[] indexListOfTextFields;
  protected int byteArrayLengthForMemoryControl;
  protected long totalByteArrayLengthLimit;
  protected long totalByteArrayLength;

  /**
   * Construct a ElasticSerializableRowRecordList.
   *
   * @param dataTypes Data types of columns.
   * @param queryId Query ID.
   * @param memoryLimitInMB Memory limit.
   * @param numCacheBlock Number of cache blocks.
   * @throws QueryProcessException by SerializableRowRecordList.calculateCapacity
   */
  public ElasticSerializableRowRecordList(
      TSDataType[] dataTypes, String queryId, float memoryLimitInMB, int numCacheBlock)
      throws QueryProcessException {
    this.dataTypes = dataTypes;
    this.queryId = queryId;
    this.memoryLimitInMB = memoryLimitInMB;
    int allocatableCapacity =
        SerializableRowRecordList.calculateCapacity(
            dataTypes,
            memoryLimitInMB,
            SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL);
    internalRowRecordListCapacity = allocatableCapacity / numCacheBlock;
    if (internalRowRecordListCapacity == 0) {
      numCacheBlock = 1;
      internalRowRecordListCapacity = allocatableCapacity;
    }
    this.numCacheBlock = numCacheBlock;

    cache = new ElasticSerializableRowRecordList.LRUCache(numCacheBlock);
    rowLists = new ArrayList<>();
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
  }

  protected ElasticSerializableRowRecordList(
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

    cache = new ElasticSerializableRowRecordList.LRUCache(numCacheBlock);
    rowLists = new ArrayList<>();
    bitMaps = new ArrayList<>();
    blockCount = new ArrayList<>();

    rowCount = 0;
    evictionUpperBound = 0;

    disableMemoryControl = true;
  }

  public int size() {
    return rowCount;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public int getTotalBlockCount() {
    int previous = blockCount.stream().mapToInt(Integer::intValue).sum();
    int current = 0;
    if (rowLists.size() != 0) {
      current = rowLists.get(rowLists.size() - 1).getBlockCount();
    }

    return previous + current;
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

  public Column[] getColumns(int blockIndex) throws IOException {
    int externalIndex = 0;
    int internalIndex = blockIndex;
    while (externalIndex < blockCount.size() && internalIndex >= blockCount.get(externalIndex)) {
      internalIndex -= blockCount.get(externalIndex);
      externalIndex++;
    }

    return cache.get(externalIndex).getColumns(internalIndex);
  }

  public Column[] getColumns(int externalIndex, int internalIndex) throws IOException {
    return cache.get(externalIndex).getColumns(internalIndex);
  }

  public RowListForwardIterator constructIterator() {
    return new RowListForwardIterator() {
      private int externalIndex;
      private int internalIndex = -1;

      @Override
      public Column[] currentBlock() throws IOException {
        return getColumns(externalIndex, internalIndex);
      }

      @Override
      public boolean hasNext() {
        // First time call, rowList has no data
        if (rowLists.size() == 0) {
          return false;
        }
        return externalIndex + 1 < rowLists.size()
            || internalIndex + 1 < rowLists.get(externalIndex).getBlockCount();
      }

      @Override
      public void next() {
        if (internalIndex + 1 == rowLists.get(externalIndex).getBlockCount()) {
          internalIndex = 0;
          externalIndex++;
        } else {
          internalIndex++;
        }
      }
    };
  }

  /** true if any field except the timestamp in the current row is null. */
  public boolean fieldsHasAnyNull(int index) {
    return bitMaps
        .get(index / internalRowRecordListCapacity)
        .isMarked(index % internalRowRecordListCapacity);
  }

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
      totalByteArrayLengthLimit +=
          (long) indexListOfTextFields.length * byteArrayLengthForMemoryControl;

      if (rowRecord == null) {
        totalByteArrayLength +=
            (long) indexListOfTextFields.length * byteArrayLengthForMemoryControl;
      } else {
        for (int indexListOfTextField : indexListOfTextFields) {
          Binary binary = (Binary) rowRecord[indexListOfTextField];
          totalByteArrayLength += binary == null ? 0 : binary.getLength();
        }
        checkMemoryUsage();
      }
    }
  }

  public void put(Column[] columns) throws IOException {
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
          insertedColumns[i] = columns[i].getRegion(begin, consumed);
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
    // TODO: memory control for binary column
  }

  private void putNulls(int nullCount) throws IOException {
    checkExpansion();

    while (nullCount > 0) {
      int consumed =
          Math.min(nullCount, internalRowRecordListCapacity)
              - rowCount % internalRowRecordListCapacity;

      cache.get(rowCount / internalRowRecordListCapacity).putNulls(consumed);
      markBitMapByGivenNullCount(consumed);

      nullCount -= consumed;
      rowCount += consumed;

      if (nullCount > 0) {
        doExpansion();
      }
    }
  }

  private void checkExpansion() {
    if (rowCount % internalRowRecordListCapacity == 0) {
      doExpansion();
    }
  }

  private void doExpansion() {
    if (rowLists.size() > 1) {
      // Add last row record list's block count
      blockCount.add(rowLists.get(rowLists.size() - 1).getBlockCount());
    }

    rowLists.add(
        SerializableRowRecordList.newSerializableRowRecordList(
            queryId, dataTypes, internalRowRecordListCapacity));
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

  private void markBitMapByGivenNullCount(int nullCount) {
    BitMap bitmap = bitMaps.get(rowCount / internalRowRecordListCapacity);

    int offset = rowCount % internalRowRecordListCapacity;
    for (int i = 0; i < nullCount; i++) {
      bitmap.mark(offset + i);
    }
  }

  protected void checkMemoryUsage() throws IOException, QueryProcessException {
    if (rowCount % MEMORY_CHECK_THRESHOLD != 0
        || totalByteArrayLength <= totalByteArrayLengthLimit) {
      return;
    }

    int newByteArrayLengthForMemoryControl = byteArrayLengthForMemoryControl;
    while ((long) newByteArrayLengthForMemoryControl * rowCount < totalByteArrayLength) {
      newByteArrayLengthForMemoryControl *= 2;
    }
    int newInternalTVListCapacity =
        SerializableRowRecordList.calculateCapacity(
                dataTypes, memoryLimitInMB, newByteArrayLengthForMemoryControl)
            / numCacheBlock;
    if (0 < newInternalTVListCapacity) {
      applyNewMemoryControlParameters(
          newByteArrayLengthForMemoryControl, newInternalTVListCapacity);
      return;
    }

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
        SerializableRowRecordList.calculateCapacity(
                dataTypes, memoryLimitInMB, newByteArrayLengthForMemoryControl)
            / numCacheBlock;
    if (0 < newInternalTVListCapacity) {
      applyNewMemoryControlParameters(
          newByteArrayLengthForMemoryControl, newInternalTVListCapacity);
      return;
    }

    throw new QueryProcessException("Memory is not enough for current query.");
  }

  protected void applyNewMemoryControlParameters(
      int newByteArrayLengthForMemoryControl, int newInternalRowRecordListCapacity)
      throws IOException {
    ElasticSerializableRowRecordList newElasticSerializableRowRecordList =
        new ElasticSerializableRowRecordList(
            dataTypes, queryId, memoryLimitInMB, newInternalRowRecordListCapacity, numCacheBlock);

    newElasticSerializableRowRecordList.evictionUpperBound = evictionUpperBound;
    int internalListEvictionUpperBound = evictionUpperBound / newInternalRowRecordListCapacity;
    for (int i = 0; i < internalListEvictionUpperBound; ++i) {
      newElasticSerializableRowRecordList.rowLists.add(null);
      newElasticSerializableRowRecordList.bitMaps.add(null);
    }
    newElasticSerializableRowRecordList.rowCount =
        internalListEvictionUpperBound * newInternalRowRecordListCapacity;
    newElasticSerializableRowRecordList.putNulls(
        evictionUpperBound - newElasticSerializableRowRecordList.rowCount);

    ColumnBuilder[] builders = constructColumnBuilders(dataTypes, rowCount - evictionUpperBound);
    for (int i = evictionUpperBound; i < rowCount; ++i) {
      Object[] row = getRowRecord(i);
      appendRowInColumnBuilders(dataTypes, row, builders);
    }
    Column[] columns = buildColumnsByBuilders(dataTypes, builders);
    newElasticSerializableRowRecordList.put(columns);

    internalRowRecordListCapacity = newInternalRowRecordListCapacity;
    cache = newElasticSerializableRowRecordList.cache;
    rowLists = newElasticSerializableRowRecordList.rowLists;
    bitMaps = newElasticSerializableRowRecordList.bitMaps;

    byteArrayLengthForMemoryControl = newByteArrayLengthForMemoryControl;
    totalByteArrayLengthLimit =
        (long) rowCount * indexListOfTextFields.length * byteArrayLengthForMemoryControl;
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

    SerializableRowRecordList get(int targetIndex) throws IOException {
      if (!containsKey(targetIndex)) {
        if (cacheCapacity <= super.size()) {
          int lastIndex = getLast();
          if (lastIndex < evictionUpperBound / internalRowRecordListCapacity) {
            rowLists.set(lastIndex, null);
            bitMaps.set(lastIndex, null);
          } else {
            rowLists.get(lastIndex).serialize();
          }
        }
        rowLists.get(targetIndex).deserialize();
      }
      putKey(targetIndex);
      return rowLists.get(targetIndex);
    }
  }
}
