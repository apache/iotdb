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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * This is a test utility class.
 */
public class FakedSeriesReaderByTimestamp implements IReaderByTimestamp {

  private Iterator<TimeValuePair> iterator;
  private boolean hasCachedTimeValuePair = false;
  private TimeValuePair cachedTimeValuePair;

  public FakedSeriesReaderByTimestamp(long startTime, int size, int interval, int modValue) {
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
  public Object getValueInTimestamp(long timestamp) {
    if (hasCachedTimeValuePair) {
      if (timestamp == cachedTimeValuePair.getTimestamp()) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair.getValue().getValue();
      } else if (timestamp > cachedTimeValuePair.getTimestamp()) {
        hasCachedTimeValuePair = false;
      } else {
        return null;
      }
    }
    while (iterator.hasNext()) {
      cachedTimeValuePair = iterator.next();
      if (timestamp == cachedTimeValuePair.getTimestamp()) {
        return cachedTimeValuePair.getValue().getValue();
      } else if (timestamp < cachedTimeValuePair.getTimestamp()) {
        hasCachedTimeValuePair = true;
        break;
      }
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    return hasCachedTimeValuePair || iterator.hasNext();
  }
}

