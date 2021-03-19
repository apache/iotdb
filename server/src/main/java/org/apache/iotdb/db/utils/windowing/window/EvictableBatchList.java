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

package org.apache.iotdb.db.utils.windowing.window;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;

public class EvictableBatchList {

  private static final int INTERNAL_BATCH_SIZE =
      TSFileConfig.ARRAY_CAPACITY_THRESHOLD * TSFileConfig.ARRAY_CAPACITY_THRESHOLD;

  private final TSDataType dataType;

  private List<BatchData> batchList;
  private int size;

  private int actualOuterIndexAt0;

  public EvictableBatchList(TSDataType dataType) {
    this.dataType = dataType;
    batchList = new ArrayList<>();
    size = 0;
    actualOuterIndexAt0 = 0;
  }

  public synchronized void putInt(long t, int v) {
    if (size % INTERNAL_BATCH_SIZE == 0) {
      batchList.add(new BatchData(dataType));
    }

    batchList.get(size / INTERNAL_BATCH_SIZE - actualOuterIndexAt0).putInt(t, v);
    ++size;
  }

  public synchronized void putLong(long t, long v) {
    if (size % INTERNAL_BATCH_SIZE == 0) {
      batchList.add(new BatchData(dataType));
    }

    batchList.get(size / INTERNAL_BATCH_SIZE - actualOuterIndexAt0).putLong(t, v);
    ++size;
  }

  public synchronized void putFloat(long t, float v) {
    if (size % INTERNAL_BATCH_SIZE == 0) {
      batchList.add(new BatchData(dataType));
    }

    batchList.get(size / INTERNAL_BATCH_SIZE - actualOuterIndexAt0).putFloat(t, v);
    ++size;
  }

  public synchronized void putDouble(long t, double v) {
    if (size % INTERNAL_BATCH_SIZE == 0) {
      batchList.add(new BatchData(dataType));
    }

    batchList.get(size / INTERNAL_BATCH_SIZE - actualOuterIndexAt0).putDouble(t, v);
    ++size;
  }

  public synchronized void putBoolean(long t, boolean v) {
    if (size % INTERNAL_BATCH_SIZE == 0) {
      batchList.add(new BatchData(dataType));
    }

    batchList.get(size / INTERNAL_BATCH_SIZE - actualOuterIndexAt0).putBoolean(t, v);
    ++size;
  }

  public synchronized void putBinary(long t, Binary v) {
    if (size % INTERNAL_BATCH_SIZE == 0) {
      batchList.add(new BatchData(dataType));
    }

    batchList.get(size / INTERNAL_BATCH_SIZE - actualOuterIndexAt0).putBinary(t, v);
    ++size;
  }

  public synchronized long getTimeByIndex(int index) {
    return batchList
        .get(index / INTERNAL_BATCH_SIZE - actualOuterIndexAt0)
        .getTimeByIndex(index % INTERNAL_BATCH_SIZE);
  }

  public synchronized int getIntByIndex(int index) {
    return batchList
        .get(index / INTERNAL_BATCH_SIZE - actualOuterIndexAt0)
        .getIntByIndex(index % INTERNAL_BATCH_SIZE);
  }

  public synchronized long getLongByIndex(int index) {
    return batchList
        .get(index / INTERNAL_BATCH_SIZE - actualOuterIndexAt0)
        .getLongByIndex(index % INTERNAL_BATCH_SIZE);
  }

  public synchronized float getFloatByIndex(int index) {
    return batchList
        .get(index / INTERNAL_BATCH_SIZE - actualOuterIndexAt0)
        .getFloatByIndex(index % INTERNAL_BATCH_SIZE);
  }

  public synchronized double getDoubleByIndex(int index) {
    return batchList
        .get(index / INTERNAL_BATCH_SIZE - actualOuterIndexAt0)
        .getDoubleByIndex(index % INTERNAL_BATCH_SIZE);
  }

  public synchronized boolean getBooleanByIndex(int index) {
    return batchList
        .get(index / INTERNAL_BATCH_SIZE - actualOuterIndexAt0)
        .getBooleanByIndex(index % INTERNAL_BATCH_SIZE);
  }

  public synchronized Binary getBinaryByIndex(int index) {
    return batchList
        .get(index / INTERNAL_BATCH_SIZE - actualOuterIndexAt0)
        .getBinaryByIndex(index % INTERNAL_BATCH_SIZE);
  }

  /** @param evictionUpperBound valid elements [evictionUpperBound, size) */
  public void setEvictionUpperBound(int evictionUpperBound) {
    int outerEvictionUpperBound = evictionUpperBound / INTERNAL_BATCH_SIZE;
    if (actualOuterIndexAt0 < outerEvictionUpperBound) {
      doEviction(outerEvictionUpperBound);
    }
  }

  private synchronized void doEviction(int outerEvictionUpperBound) {
    batchList =
        new ArrayList<>(
            batchList.subList(outerEvictionUpperBound - actualOuterIndexAt0, batchList.size()));
    actualOuterIndexAt0 = outerEvictionUpperBound;
  }

  public int size() {
    return size;
  }

  public TSDataType getDataType() {
    return dataType;
  }
}
