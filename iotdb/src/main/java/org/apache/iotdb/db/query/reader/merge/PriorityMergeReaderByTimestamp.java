/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.reader.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Usage: Get value in timestamp by sorting time-value pair in multiple readers with time and
 * priority. (1) merge multiple chunk group readers in the unsequence file (2）merge sequence reader,
 * unsequence reader and mem reader
 * </p>
 */
public class PriorityMergeReaderByTimestamp implements EngineReaderByTimeStamp {

  private List<EngineReaderByTimeStamp> readerList = new ArrayList<>();
  private List<Integer> priorityList = new ArrayList<>();

  /**
   * The bigger the priority value is, the higher the priority of this reader is.
   */
  public void addReaderWithPriority(EngineReaderByTimeStamp reader, int priority) {
    int size = readerList.size();
    boolean hasAdded = false;

    //sort by priority, elements with low priority are at front of list.
    for (int i = 0; i < size; i++) {
      if (priority < priorityList.get(i)) {
        readerList.set(i, reader);
        priorityList.set(i, priority);
        hasAdded = true;
      }
    }
    if (!hasAdded) {
      readerList.add(reader);
      priorityList.add(priority);
    }
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    Object value = null;
    for (int i = readerList.size() - 1; i >= 0; i--) {
      value = readerList.get(i).getValueInTimestamp(timestamp);
      if (value != null) {
        return value;
      }
    }
    return value;
  }

  @Override
  public void close() throws IOException {
    for (EngineReaderByTimeStamp reader : readerList) {
      reader.close();
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    for (int i = readerList.size() - 1; i >= 0; i--) {
      if (readerList.get(i).hasNext()) {
        return true;
      }
    }
    return false;
  }

}
