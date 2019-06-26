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
package org.apache.iotdb.db.engine.memtable;

import java.util.List;
import org.apache.iotdb.db.utils.TimeValuePair;

public class DeduplicatedSortedData {
  private List<TimeValuePair> timeValuePairs;

  private int index;

  private int length;

  private TimeValuePair cachedTimeValuePair;

  private boolean hasCached;

  private long timeOffset;

  public DeduplicatedSortedData(List<TimeValuePair> timeValuePairs, long timeOffset) {
    this.timeValuePairs = timeValuePairs;
    this.timeOffset = timeOffset;
    this.timeValuePairs.sort(TimeValuePair::compareTo);
    this.index = 0;
    this.length = timeValuePairs.size();
  }

  public boolean hasNext(){
    if(!hasCached) {
      cachedTimeValuePair = null;
      while (index < length) {
        if (cachedTimeValuePair == null || cachedTimeValuePair.getTimestamp() == timeValuePairs
            .get(index).getTimestamp()) {
          cachedTimeValuePair = timeValuePairs.get(index++);
          if (cachedTimeValuePair.getTimestamp() < timeOffset)
            continue;
          hasCached = true;
        } else {
          break;
        }
      }
    }
    return hasCached;
  }

  public TimeValuePair next(){
    if(!hasCached){
      hasNext();
    }
    hasCached = false;
    return cachedTimeValuePair;
  }
}
