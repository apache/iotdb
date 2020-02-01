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

package org.apache.iotdb.db.query.reader.universal;

import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.IPointReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class PriorityMergeReaderByTimestampTest {

  @Test
  public void test() throws IOException {
    IReaderByTimestamp reader1 = new FakedReaderByTimestamp(100, 200, 5, 11);
    IReaderByTimestamp reader2 = new FakedReaderByTimestamp(850, 200, 7, 19);
    IReaderByTimestamp reader3 = new FakedReaderByTimestamp(1080, 200, 13, 31);

    PriorityMergeReaderByTimestamp priorityReader = new PriorityMergeReaderByTimestamp();
    priorityReader.addReaderWithPriority(reader1, 1);
    priorityReader.addReaderWithPriority(reader2, 2);
    priorityReader.addReaderWithPriority(reader3, 3);

    Random random = new Random();
    for (long time = 4; time < 1080 + 200 * 13 + 600; ) {
      Long value = (Long) priorityReader.getValueInTimestamp(time);
//      if (time < 1080 + 199 * 13) {
//        Assert.assertTrue(priorityReader.hasNext());
//      }

      //System.out.println("time = " + time + " value = " + value);
      if (time < 100) {
        // null
        Assert.assertNull(value);
      } else if (time < 850) {
        // reader 1
        if ((time - 100) % 5 == 0) {
          Assert.assertEquals(time % 11, value.longValue());
        }
      } else if (time < 1080) {
        // reader 2, reader 1
        if (time >= 850 && (time - 850) % 7 == 0) {
          Assert.assertEquals(time % 19, value.longValue());
        } else if (time < 1100 && (time - 100) % 5 == 0) {
          Assert.assertEquals(time % 11, value.longValue());
        } else {
          Assert.assertNull(value);
        }

      } else if (time < 1080 + 200 * 13) {
        // reader 3, reader 2, reader 1
        if (time >= 1080 && (time - 1080) % 13 == 0) {
          Assert.assertEquals(time % 31, value.longValue());
        } else if (time < 850 + 200 * 7 && (time - 850) % 7 == 0) {
          Assert.assertEquals(time % 19, value.longValue());
        } else if (time < 1100 && (time - 100) % 5 == 0) {
          Assert.assertEquals(time % 11, value.longValue());
        } else {
          Assert.assertNull(value);
        }
      } else {
        // null
        Assert.assertNull(value);
      }
      time += random.nextInt(50) + 1;
    }

  }

  public static class FakedReaderByTimestamp implements IReaderByTimestamp,
          IPointReader {

    private Iterator<TimeValuePair> iterator;
    private long currentTimeStamp = Long.MIN_VALUE;
    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;

    public FakedReaderByTimestamp(long startTime, int size, int interval,
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
    public boolean hasNextTimeValuePair() {
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
    public TimeValuePair nextTimeValuePair() throws IOException {
      if (hasCachedTimeValuePair) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair;
      } else {
        throw new IOException(" to end! " + iterator.next());
      }
    }

    @Override
    public TimeValuePair currentTimeValuePair() throws IOException {
      if (hasCachedTimeValuePair) {
        return cachedTimeValuePair;
      } else {
        throw new IOException(" to end! " + iterator.next());
      }
    }


    @Override
    public void close() {
    }

    @Override
    public Object getValueInTimestamp(long timestamp) throws IOException {
      this.currentTimeStamp = timestamp;
      if (hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() == timestamp) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair.getValue().getValue();
      }

      if (hasNext()) {
        cachedTimeValuePair = nextTimeValuePair();
        if (cachedTimeValuePair.getTimestamp() == timestamp) {
          return cachedTimeValuePair.getValue().getValue();
        } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
          hasCachedTimeValuePair = true;
        }
      }
      return null;
    }

    private boolean hasNext() {
      return hasNextTimeValuePair();
    }
  }
}