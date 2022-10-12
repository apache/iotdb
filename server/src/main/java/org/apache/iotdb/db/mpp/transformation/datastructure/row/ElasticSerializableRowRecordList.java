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

package org.apache.iotdb.db.mpp.transformation.datastructure.row;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.dag.util.InputRowUtils;
import org.apache.iotdb.db.mpp.transformation.datastructure.Cache;
import org.apache.iotdb.db.mpp.transformation.datastructure.SerializableList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An elastic list of records that implements memory control using LRU strategy. */
public class ElasticSerializableRowRecordList {

  protected static final int MEMORY_CHECK_THRESHOLD = 1000;

  protected TSDataType[] dataTypes;
  protected long queryId;
  protected float memoryLimitInMB;
  protected int internalRowRecordListCapacity;
  protected int numCacheBlock;

  protected LRUCache cache;
  protected List<SerializableRowRecordList> rowRecordLists;
  /** Mark bitMaps of correct index when one row has at least one null field */
  protected List<BitMap> bitMaps;

  protected int size;
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
   */
  public ElasticSerializableRowRecordList(
      TSDataType[] dataTypes, long queryId, float memoryLimitInMB, int numCacheBlock)
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
    rowRecordLists = new ArrayList<>();
    bitMaps = new ArrayList<>();
    size = 0;
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
      long queryId,
      float memoryLimitInMB,
      int internalRowRecordListCapacity,
      int numCacheBlock) {
    this.dataTypes = dataTypes;
    this.queryId = queryId;
    this.memoryLimitInMB = memoryLimitInMB;
    this.internalRowRecordListCapacity = internalRowRecordListCapacity;
    this.numCacheBlock = numCacheBlock;

    cache = new ElasticSerializableRowRecordList.LRUCache(numCacheBlock);
    rowRecordLists = new ArrayList<>();
    bitMaps = new ArrayList<>();
    size = 0;
    evictionUpperBound = 0;

    disableMemoryControl = true;
  }

  public int size() {
    return size;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public long getTime(int index) throws IOException {
    return cache
        .get(index / internalRowRecordListCapacity)
        .getTime(index % internalRowRecordListCapacity);
  }

  public Object[] getRowRecord(int index) throws IOException {
    return cache
        .get(index / internalRowRecordListCapacity)
        .getRowRecord(index % internalRowRecordListCapacity);
  }

  /** true if any field except the timestamp in the current row is null */
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
   * directly
   */
  private void put(Object[] rowRecord, boolean hasNullField)
      throws IOException, QueryProcessException {
    checkExpansion();
    cache.get(size / internalRowRecordListCapacity).put(rowRecord);
    if (hasNullField) {
      bitMaps.get(size / internalRowRecordListCapacity).mark(size % internalRowRecordListCapacity);
    }
    ++size;

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

  private void checkExpansion() {
    if (size % internalRowRecordListCapacity == 0) {
      rowRecordLists.add(
          SerializableRowRecordList.newSerializableRowRecordList(
              queryId, dataTypes, internalRowRecordListCapacity));
      bitMaps.add(new BitMap(internalRowRecordListCapacity));
    }
  }

  protected void checkMemoryUsage() throws IOException, QueryProcessException {
    if (size % MEMORY_CHECK_THRESHOLD != 0 || totalByteArrayLength <= totalByteArrayLengthLimit) {
      return;
    }

    int newByteArrayLengthForMemoryControl = byteArrayLengthForMemoryControl;
    while ((long) newByteArrayLengthForMemoryControl * size < totalByteArrayLength) {
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
                / size
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
      throws IOException, QueryProcessException {
    ElasticSerializableRowRecordList newElasticSerializableRowRecordList =
        new ElasticSerializableRowRecordList(
            dataTypes, queryId, memoryLimitInMB, newInternalRowRecordListCapacity, numCacheBlock);

    newElasticSerializableRowRecordList.evictionUpperBound = evictionUpperBound;
    int internalListEvictionUpperBound = evictionUpperBound / newInternalRowRecordListCapacity;
    for (int i = 0; i < internalListEvictionUpperBound; ++i) {
      newElasticSerializableRowRecordList.rowRecordLists.add(null);
      newElasticSerializableRowRecordList.bitMaps.add(null);
    }
    newElasticSerializableRowRecordList.size =
        internalListEvictionUpperBound * newInternalRowRecordListCapacity;
    for (int i = newElasticSerializableRowRecordList.size; i < evictionUpperBound; ++i) {
      newElasticSerializableRowRecordList.put(null);
    }
    for (int i = evictionUpperBound; i < size; ++i) {
      newElasticSerializableRowRecordList.put(getRowRecord(i), fieldsHasAnyNull(i));
    }

    internalRowRecordListCapacity = newInternalRowRecordListCapacity;
    cache = newElasticSerializableRowRecordList.cache;
    rowRecordLists = newElasticSerializableRowRecordList.rowRecordLists;
    bitMaps = newElasticSerializableRowRecordList.bitMaps;

    byteArrayLengthForMemoryControl = newByteArrayLengthForMemoryControl;
    totalByteArrayLengthLimit =
        (long) size * indexListOfTextFields.length * byteArrayLengthForMemoryControl;
  }

  /**
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
        if (cacheCapacity <= size()) {
          int lastIndex = getLast();
          if (lastIndex < evictionUpperBound / internalRowRecordListCapacity) {
            rowRecordLists.set(lastIndex, null);
            bitMaps.set(lastIndex, null);
          } else {
            rowRecordLists.get(lastIndex).serialize();
          }
        }
        rowRecordLists.get(targetIndex).deserialize();
      }
      putKey(targetIndex);
      return rowRecordLists.get(targetIndex);
    }
  }
}
