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

package org.apache.iotdb.db.query.udf.datastructure.row;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.datastructure.Cache;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.query.udf.datastructure.SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;

public class ElasticSerializableRowRecordList {

  protected static final int MEMORY_CHECK_THRESHOLD = 1000;

  protected TSDataType[] dataTypes;
  protected long queryId;
  protected float memoryLimitInMB;
  protected int internalRowRecordListCapacity;
  protected int cacheSize;

  protected LRUCache cache;
  protected List<SerializableRowRecordList> rowRecordLists;
  protected int size;
  protected int evictionUpperBound;

  protected boolean disableMemoryControl;
  protected int[] indexListOfTextFields;
  protected int byteArrayLengthForMemoryControl;
  protected long totalByteArrayLengthLimit;
  protected long totalByteArrayLength;

  public ElasticSerializableRowRecordList(
      TSDataType[] dataTypes, long queryId, float memoryLimitInMB, int cacheSize)
      throws QueryProcessException {
    this.dataTypes = dataTypes;
    this.queryId = queryId;
    this.memoryLimitInMB = memoryLimitInMB;
    int allocatableCapacity =
        SerializableRowRecordList.calculateCapacity(
            dataTypes, memoryLimitInMB, INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL);
    internalRowRecordListCapacity = allocatableCapacity / cacheSize;
    if (internalRowRecordListCapacity == 0) {
      cacheSize = 1;
      internalRowRecordListCapacity = allocatableCapacity;
    }
    this.cacheSize = cacheSize;

    cache = new ElasticSerializableRowRecordList.LRUCache(cacheSize);
    rowRecordLists = new ArrayList<>();
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
    byteArrayLengthForMemoryControl = INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
    totalByteArrayLengthLimit = 0;
    totalByteArrayLength = 0;
  }

  protected ElasticSerializableRowRecordList(
      TSDataType[] dataTypes,
      long queryId,
      float memoryLimitInMB,
      int internalRowRecordListCapacity,
      int cacheSize) {
    this.dataTypes = dataTypes;
    this.queryId = queryId;
    this.memoryLimitInMB = memoryLimitInMB;
    this.internalRowRecordListCapacity = internalRowRecordListCapacity;
    this.cacheSize = cacheSize;

    cache = new ElasticSerializableRowRecordList.LRUCache(cacheSize);
    rowRecordLists = new ArrayList<>();
    size = 0;
    evictionUpperBound = 0;

    disableMemoryControl = true;
  }

  public int size() {
    return size;
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

  public void put(Object[] rowRecord) throws IOException, QueryProcessException {
    checkExpansion();
    cache.get(size / internalRowRecordListCapacity).put(rowRecord);
    ++size;

    if (!disableMemoryControl) {
      totalByteArrayLengthLimit +=
          (long) indexListOfTextFields.length * byteArrayLengthForMemoryControl;

      if (rowRecord == null) {
        totalByteArrayLength += indexListOfTextFields.length * byteArrayLengthForMemoryControl;
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
    }
  }

  protected void checkMemoryUsage() throws IOException, QueryProcessException {
    if (size % MEMORY_CHECK_THRESHOLD != 0 || totalByteArrayLength <= totalByteArrayLengthLimit) {
      return;
    }

    int newByteArrayLengthForMemoryControl = byteArrayLengthForMemoryControl;
    while (newByteArrayLengthForMemoryControl * size < totalByteArrayLength) {
      newByteArrayLengthForMemoryControl *= 2;
    }
    int newInternalTVListCapacity =
        SerializableRowRecordList.calculateCapacity(
                dataTypes, memoryLimitInMB, newByteArrayLengthForMemoryControl)
            / cacheSize;
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
                / INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL);
    newByteArrayLengthForMemoryControl =
        byteArrayLengthForMemoryControl
            + 2 * (delta + 1) * INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
    newInternalTVListCapacity =
        SerializableRowRecordList.calculateCapacity(
                dataTypes, memoryLimitInMB, newByteArrayLengthForMemoryControl)
            / cacheSize;
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
            dataTypes, queryId, memoryLimitInMB, newInternalRowRecordListCapacity, cacheSize);

    newElasticSerializableRowRecordList.evictionUpperBound = evictionUpperBound;
    int internalListEvictionUpperBound = evictionUpperBound / newInternalRowRecordListCapacity;
    for (int i = 0; i < internalListEvictionUpperBound; ++i) {
      newElasticSerializableRowRecordList.rowRecordLists.add(null);
    }
    newElasticSerializableRowRecordList.size =
        internalListEvictionUpperBound * newInternalRowRecordListCapacity;
    for (int i = newElasticSerializableRowRecordList.size; i < evictionUpperBound; ++i) {
      newElasticSerializableRowRecordList.put(null);
    }
    for (int i = evictionUpperBound; i < size; ++i) {
      newElasticSerializableRowRecordList.put(getRowRecord(i));
    }

    internalRowRecordListCapacity = newInternalRowRecordListCapacity;
    cache = newElasticSerializableRowRecordList.cache;
    rowRecordLists = newElasticSerializableRowRecordList.rowRecordLists;

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
      if (!removeFirstOccurrence(targetIndex)) {
        if (cacheCapacity <= cacheSize) {
          int lastIndex = removeLast();
          if (lastIndex < evictionUpperBound / internalRowRecordListCapacity) {
            rowRecordLists.set(lastIndex, null);
          } else {
            rowRecordLists.get(lastIndex).serialize();
          }
        }
        rowRecordLists.get(targetIndex).deserialize();
      }
      addFirst(targetIndex);
      return rowRecordLists.get(targetIndex);
    }
  }
}
