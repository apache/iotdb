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

import static org.apache.iotdb.db.query.udf.datastructure.SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class ElasticSerializableRowRecordList {

  public static final float DEFAULT_MEMORY_USAGE_LIMIT = 100;
  public static final int DEFAULT_CACHE_SIZE = 3;

  protected static final int MEMORY_CHECK_THRESHOLD = 1000;

  protected static final String UNIQUE_ID_MAGIC_STRING = "__ROW__";
  protected static final String UNIQUE_ID_STRING_PATTERN = "%s" + UNIQUE_ID_MAGIC_STRING + "%d";

  protected TSDataType[] dataTypes;
  protected long queryId;
  protected String uniqueId;
  protected float memoryLimitInMB;
  protected int internalRowRecordListCapacity;
  protected int cacheSize;

  protected LRUCache cache;
  protected List<SerializableRowRecordList> rowRecordLists;
  protected int size;
  protected int evictionUpperBound;

  protected boolean disableMemoryControl;
  protected List<Integer> indexListOfTextFields;
  protected int byteArrayLengthForMemoryControl;
  protected long totalByteArrayLengthLimit;
  protected long totalByteArrayLength;
  protected int uniqueIdVersion;

  public ElasticSerializableRowRecordList(TSDataType[] dataTypes, long queryId, String uniqueId,
      float memoryLimitInMB, int cacheSize) throws QueryProcessException {
    this.dataTypes = dataTypes;
    this.queryId = queryId;
    this.uniqueId = uniqueId;
    this.memoryLimitInMB = memoryLimitInMB;
    int allocatableCapacity = SerializableRowRecordList
        .calculateCapacity(dataTypes, memoryLimitInMB,
            INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL);
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

    disableMemoryControl = false;
    indexListOfTextFields = new ArrayList<>();
    for (int i = 0; i < dataTypes.length; ++i) {
      if (dataTypes[i].equals(TSDataType.TEXT)) {
        indexListOfTextFields.add(i);
      }
    }
    byteArrayLengthForMemoryControl = INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
    totalByteArrayLengthLimit = 0;
    totalByteArrayLength = 0;
    uniqueIdVersion = 0;
  }

  protected ElasticSerializableRowRecordList(TSDataType[] dataTypes, long queryId, String uniqueId,
      float memoryLimitInMB, int internalRowRecordListCapacity, int cacheSize) {
    this.dataTypes = dataTypes;
    this.queryId = queryId;
    this.uniqueId = uniqueId;
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
    return cache.get(index / internalRowRecordListCapacity)
        .getTime(index % internalRowRecordListCapacity);
  }

  public RowRecord getRowRecord(int index) throws IOException {
    return cache.get(index / internalRowRecordListCapacity)
        .getRowRecord(index % internalRowRecordListCapacity);
  }

  public void put(RowRecord rowRecord) throws IOException, QueryProcessException {
    checkExpansion();
    cache.get(size / internalRowRecordListCapacity).put(rowRecord);
    ++size;

    if (!disableMemoryControl) {
      totalByteArrayLengthLimit += indexListOfTextFields.size() * byteArrayLengthForMemoryControl;
      List<Field> fields = rowRecord.getFields();
      for (Integer indexListOfTextField : indexListOfTextFields) {
        totalByteArrayLength += fields.get(indexListOfTextField).getBinaryV().getLength();
      }
      checkMemoryUsage();
    }
  }

  private void checkExpansion() {
    if (size % internalRowRecordListCapacity == 0) {
      int index = rowRecordLists.size();
      rowRecordLists.add(SerializableRowRecordList
          .newSerializableRowRecordList(dataTypes, queryId, uniqueId, index));
    }
  }

  protected void checkMemoryUsage() throws IOException, QueryProcessException {
    if (size % MEMORY_CHECK_THRESHOLD != 0 || indexListOfTextFields.isEmpty()
        || totalByteArrayLength <= totalByteArrayLengthLimit) {
      return;
    }

    int newByteArrayLengthForMemoryControl = byteArrayLengthForMemoryControl;
    while (newByteArrayLengthForMemoryControl * size < totalByteArrayLength) {
      newByteArrayLengthForMemoryControl *= 2;
    }
    int newInternalTVListCapacity = SerializableRowRecordList
        .calculateCapacity(dataTypes, memoryLimitInMB, newByteArrayLengthForMemoryControl)
        / cacheSize;
    if (0 < newInternalTVListCapacity) {
      applyNewMemoryControlParameters(newByteArrayLengthForMemoryControl,
          newInternalTVListCapacity);
      return;
    }

    int delta = (int) ((totalByteArrayLength - totalByteArrayLengthLimit) / size
        / indexListOfTextFields.size() / INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL);
    newByteArrayLengthForMemoryControl = byteArrayLengthForMemoryControl +
        2 * (delta + 1) * INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
    newInternalTVListCapacity = SerializableRowRecordList
        .calculateCapacity(dataTypes, memoryLimitInMB, newByteArrayLengthForMemoryControl)
        / cacheSize;
    if (0 < newInternalTVListCapacity) {
      applyNewMemoryControlParameters(newByteArrayLengthForMemoryControl,
          newInternalTVListCapacity);
      return;
    }

    throw new QueryProcessException("Memory is not enough for current query.");
  }

  protected void applyNewMemoryControlParameters(int newByteArrayLengthForMemoryControl,
      int newInternalRowRecordListCapacity) throws IOException, QueryProcessException {
    String newUniqueId = generateNewUniqueId();
    ElasticSerializableRowRecordList newElasticSerializableRowRecordList = new ElasticSerializableRowRecordList(
        dataTypes, queryId, newUniqueId, memoryLimitInMB, newInternalRowRecordListCapacity,
        cacheSize);

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

    uniqueId = newUniqueId;
    internalRowRecordListCapacity = newInternalRowRecordListCapacity;
    cache = newElasticSerializableRowRecordList.cache;
    rowRecordLists = newElasticSerializableRowRecordList.rowRecordLists;

    byteArrayLengthForMemoryControl = newByteArrayLengthForMemoryControl;
    totalByteArrayLengthLimit =
        (long) size * indexListOfTextFields.size() * byteArrayLengthForMemoryControl;
  }

  protected String generateNewUniqueId() {
    int firstOccurrence = uniqueId.indexOf(UNIQUE_ID_MAGIC_STRING);
    return String.format(UNIQUE_ID_STRING_PATTERN, firstOccurrence == -1
        ? uniqueId : uniqueId.substring(0, firstOccurrence), uniqueIdVersion++);
  }

  public void setEvictionUpperBound(int evictionUpperBound) {
    this.evictionUpperBound = evictionUpperBound;
  }

  /**
   * <b>Note: It's not thread safe.</b>
   */
  private class LRUCache {

    private final int capacity;
    private final LinkedList<Integer> cache;

    LRUCache(int capacity) {
      this.capacity = capacity;
      cache = new LinkedList<>();
    }

    SerializableRowRecordList get(int targetIndex) throws IOException {
      if (rowRecordLists.size() <= targetIndex) {
        throw new ArrayIndexOutOfBoundsException(targetIndex);
      }
      if (!cache.removeFirstOccurrence(targetIndex)) {
        if (capacity <= cache.size()) {
          int lastIndex = cache.removeLast();
          if (lastIndex < evictionUpperBound / internalRowRecordListCapacity) {
            rowRecordLists.set(lastIndex, null);
          } else {
            rowRecordLists.get(lastIndex).serialize();
          }
        }
        rowRecordLists.get(targetIndex).deserialize();
      }
      cache.addFirst(targetIndex);
      return rowRecordLists.get(targetIndex);
    }
  }
}
