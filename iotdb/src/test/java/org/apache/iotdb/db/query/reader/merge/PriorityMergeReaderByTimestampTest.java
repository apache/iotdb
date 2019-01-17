/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.reader.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.junit.Assert;
import org.junit.Test;

public class PriorityMergeReaderByTimestampTest {

  @Test
  public void test() throws IOException {
    FakedPrioritySeriesReaderByTimestamp reader1 = new FakedPrioritySeriesReaderByTimestamp(100,
        200, 5, 11);
    FakedPrioritySeriesReaderByTimestamp reader2 = new FakedPrioritySeriesReaderByTimestamp(850,
        200, 7, 19);
    FakedPrioritySeriesReaderByTimestamp reader3 = new FakedPrioritySeriesReaderByTimestamp(1080,
        200, 13, 31);

    PriorityMergeReaderByTimestamp priorityReader = new PriorityMergeReaderByTimestamp();
    priorityReader.addReaderWithPriority(reader1, 1);
    priorityReader.addReaderWithPriority(reader2, 2);
    priorityReader.addReaderWithPriority(reader3, 3);

    int cnt = 0;

    Random random = new Random();
    for (long time = 4; time < 1080 + 200 * 13 + 600; ) {
      TsPrimitiveType value = priorityReader.getValueInTimestamp(time);
      // System.out.println("time = " + time + " value = " + value);
      if (time < 100) {
        // null
        Assert.assertNull(value);
      } else if (time < 850) {
        // reader 1
        if ((time - 100) % 5 == 0) {
          Assert.assertEquals(time % 11, value.getLong());
        }
      } else if (time < 1080) {
        // reader 2, reader 1
        if (time >= 850 && (time - 850) % 7 == 0) {
          Assert.assertEquals(time % 19, value.getLong());
        } else if (time < 1100 && (time - 100) % 5 == 0) {
          Assert.assertEquals(time % 11, value.getLong());
        } else {
          Assert.assertNull(value);
        }

      } else if (time < 1080 + 200 * 13) {
        // reader 3, reader 2, reader 1
        if (time >= 1080 && (time - 1080) % 13 == 0) {
          Assert.assertEquals(time % 31, value.getLong());
        } else if (time < 850 + 200 * 7 && (time - 850) % 7 == 0) {
          Assert.assertEquals(time % 19, value.getLong());
        } else if (time < 1100 && (time - 100) % 5 == 0) {
          Assert.assertEquals(time % 11, value.getLong());
        } else {
          Assert.assertNull(value);
        }
      } else {
        // null
        Assert.assertNull(value);
      }
      time += random.nextInt(50) + 1;
    }

    while (priorityReader.hasNext()) {
      TimeValuePair timeValuePair = priorityReader.next();
      long time = timeValuePair.getTimestamp();
      long value = timeValuePair.getValue().getLong();
      if (time < 850) {
        Assert.assertEquals(time % 11, value);
      } else if (time < 1080) {
        Assert.assertEquals(time % 19, value);
      } else {
        Assert.assertEquals(time % 31, value);
      }
      cnt++;
    }

  }

  public static class FakedPrioritySeriesReaderByTimestamp implements EngineReaderByTimeStamp {

    private Iterator<TimeValuePair> iterator;
    private long currentTimeStamp = Long.MIN_VALUE;
    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;

    public FakedPrioritySeriesReaderByTimestamp(long startTime, int size, int interval,
        int modValue) {
      long time = startTime;
      List<TimeValuePair> list = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        list.add(
            new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.INT64, time % modValue)));
        time += interval;
      }
      iterator = list.iterator();
    }

    @Override
    public boolean hasNext() throws IOException {
      if (hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() >= currentTimeStamp) {
        return true;
      }

      while (iterator.hasNext()) {
        cachedTimeValuePair = iterator.next();
        if (cachedTimeValuePair.getTimestamp() >= currentTimeStamp) {
          hasCachedTimeValuePair = true;
          return true;
        }
      }
      return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
      if (hasCachedTimeValuePair) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair;
      } else {
        throw new IOException(" to end! " + iterator.next());
      }
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
      next();
    }

    @Override
    public void close() {
    }

    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
      this.currentTimeStamp = timestamp;
      if (hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() == timestamp) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair.getValue();
      }

      if (hasNext()) {
        cachedTimeValuePair = next();
        if (cachedTimeValuePair.getTimestamp() == timestamp) {
          return cachedTimeValuePair.getValue();
        } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
          hasCachedTimeValuePair = true;
        }
      }
      return null;
    }

    @Override
    public boolean hasNextBatch() {
      return false;
    }

    @Override
    public BatchData nextBatch() {
      return null;
    }

    @Override
    public BatchData currentBatch() {
      return null;
    }
  }
}
