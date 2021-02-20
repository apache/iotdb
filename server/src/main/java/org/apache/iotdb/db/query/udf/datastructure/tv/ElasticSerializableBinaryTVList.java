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

package org.apache.iotdb.db.query.udf.datastructure.tv;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;

import static org.apache.iotdb.db.query.udf.datastructure.SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;

public class ElasticSerializableBinaryTVList extends ElasticSerializableTVList {

  protected static final int MEMORY_CHECK_THRESHOLD = 1000;

  protected int byteArrayLengthForMemoryControl;

  protected long totalByteArrayLengthLimit;
  protected long totalByteArrayLength;

  public ElasticSerializableBinaryTVList(long queryId, float memoryLimitInMB, int cacheSize)
      throws QueryProcessException {
    super(TSDataType.TEXT, queryId, memoryLimitInMB, cacheSize);
    byteArrayLengthForMemoryControl = INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
    totalByteArrayLengthLimit = 0;
    totalByteArrayLength = 0;
  }

  @Override
  public void putBinary(long timestamp, Binary value) throws IOException, QueryProcessException {
    super.putBinary(timestamp, value);
    totalByteArrayLengthLimit += byteArrayLengthForMemoryControl;
    totalByteArrayLength += value.getLength();
    checkMemoryUsage();
  }

  @Override
  public void putString(long timestamp, String value) throws IOException, QueryProcessException {
    Binary binary = Binary.valueOf(value);
    super.putBinary(timestamp, binary);
    totalByteArrayLengthLimit += byteArrayLengthForMemoryControl;
    totalByteArrayLength += binary.getLength();
    checkMemoryUsage();
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
        SerializableBinaryTVList.calculateCapacity(
                memoryLimitInMB, newByteArrayLengthForMemoryControl)
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
                / INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL);
    newByteArrayLengthForMemoryControl =
        byteArrayLengthForMemoryControl
            + 2 * (delta + 1) * INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
    newInternalTVListCapacity =
        SerializableBinaryTVList.calculateCapacity(
                memoryLimitInMB, newByteArrayLengthForMemoryControl)
            / cacheSize;
    if (0 < newInternalTVListCapacity) {
      applyNewMemoryControlParameters(
          newByteArrayLengthForMemoryControl, newInternalTVListCapacity);
      return;
    }

    throw new QueryProcessException("Memory is not enough for current query.");
  }

  protected void applyNewMemoryControlParameters(
      int newByteArrayLengthForMemoryControl, int newInternalTVListCapacity)
      throws IOException, QueryProcessException {
    ElasticSerializableTVList newElasticSerializableTVList =
        new ElasticSerializableTVList(
            TSDataType.TEXT, queryId, memoryLimitInMB, newInternalTVListCapacity, cacheSize);

    newElasticSerializableTVList.evictionUpperBound = evictionUpperBound;
    int internalListEvictionUpperBound = evictionUpperBound / newInternalTVListCapacity;
    for (int i = 0; i < internalListEvictionUpperBound; ++i) {
      newElasticSerializableTVList.tvLists.add(null);
    }
    newElasticSerializableTVList.size = internalListEvictionUpperBound * newInternalTVListCapacity;
    Binary empty = Binary.valueOf("");
    for (int i = newElasticSerializableTVList.size; i < evictionUpperBound; ++i) {
      newElasticSerializableTVList.putBinary(i, empty);
    }
    for (int i = evictionUpperBound; i < size; ++i) {
      newElasticSerializableTVList.putBinary(getTime(i), getBinary(i));
    }

    internalTVListCapacity = newInternalTVListCapacity;
    cache = newElasticSerializableTVList.cache;
    tvLists = newElasticSerializableTVList.tvLists;

    byteArrayLengthForMemoryControl = newByteArrayLengthForMemoryControl;
    totalByteArrayLengthLimit = (long) size * byteArrayLengthForMemoryControl;
  }
}
