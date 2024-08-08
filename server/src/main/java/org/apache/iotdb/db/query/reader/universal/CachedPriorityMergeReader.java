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

import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import java.io.IOException;

/**
 * CachedPriorityMergeReader use a cache to reduce unnecessary heap updates and increase locality.
 */
public class CachedPriorityMergeReader extends PriorityMergeReader {

  private static final int CACHE_SIZE = 100;

  private TimeValuePair[] timeValuePairCache = new TimeValuePair[CACHE_SIZE];
  private int cacheLimit = 0;
  private int cacheIdx = 0;

  private Long lastTimestamp = null;

  public CachedPriorityMergeReader(TSDataType dataType) {
    for (int i = 0; i < CACHE_SIZE; i++) {
      timeValuePairCache[i] = TimeValuePairUtils.getEmptyTimeValuePair(dataType);
    }
  }

  @Override
  public boolean hasNextTimeValuePair() {
    return cacheIdx < cacheLimit || !heap.isEmpty();
  }

  private void fetch() throws IOException {
    cacheLimit = 0;
    cacheIdx = 0;
    while (!heap.isEmpty() && cacheLimit < CACHE_SIZE) {
      Element top = heap.peek();
      if (lastTimestamp == null || top.currTime() != lastTimestamp) {
        TimeValuePairUtils.setTimeValuePair(
            top.getTimeValuePair(), timeValuePairCache[cacheLimit++]);
        lastTimestamp = top.currTime();
      }
      // remove duplicates
      while (heap.peek() != null && heap.peek().currTime() == lastTimestamp) {
        top = heap.poll();
        if (top.hasNext()) {
          top.next();
          heap.add(top);
        } else {
          top.close();
        }
      }
    }
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    TimeValuePair ret;
    if (cacheIdx < cacheLimit) {
      ret = timeValuePairCache[cacheIdx++];
    } else {
      fetch();
      ret = timeValuePairCache[cacheIdx++];
    }
    return ret;
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    if (0 <= cacheIdx && cacheIdx < cacheLimit) {
      return timeValuePairCache[cacheIdx];
    } else {
      fetch();
      return timeValuePairCache[cacheIdx];
    }
  }
}
