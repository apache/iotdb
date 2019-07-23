/**
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

package org.apache.iotdb.db.query.reader.seriesRelated;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.junit.Assert;

/**
 * This is a test utility class.
 */
public class FakedIBatchPoint implements IBatchReader {

  private Iterator<TimeValuePair> iterator;
  private BatchData batchData;
  private boolean hasCachedBatchData;
  private boolean hasEmptyBatch;
  private Random random;

  public FakedIBatchPoint(long startTime, int size, int interval, int modValue,
      boolean hasEmptyBatch) {
    long time = startTime;
    List<TimeValuePair> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(
          new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.INT64, time % modValue)));
      time += interval;
    }
    iterator = list.iterator();
    this.hasCachedBatchData = false;
    this.hasEmptyBatch = hasEmptyBatch;
    this.random = new Random();
  }

  public FakedIBatchPoint(long startTime, int size, int interval, int modValue) {
    this(startTime, size, interval, modValue, false);
  }

  @Override
  public boolean hasNext() {
    if (hasCachedBatchData) {
      return true;
    }
    if (iterator.hasNext()) {
      constructBatchData();
      hasCachedBatchData = true;
      return true;
    }
    return false;
  }

  @Override
  public BatchData nextBatch() {
    if (hasCachedBatchData) {
      hasCachedBatchData = false;
      return batchData;
    } else {
      constructBatchData();
      return batchData;
    }

  }

  private void constructBatchData() {
    int num = random.nextInt(10);
    if (!hasEmptyBatch) {
      num += 1;
    }
    batchData = new BatchData(TSDataType.INT64, true);
    while (num > 0 && iterator.hasNext()) {
      TimeValuePair timeValuePair = iterator.next();
      batchData.putTime(timeValuePair.getTimestamp());
      batchData.putLong(timeValuePair.getValue().getLong());
      num--;
    }
    if (!hasEmptyBatch) {
      Assert.assertTrue(batchData.hasNext());
    }
  }

  @Override
  public void close() {

  }

}
