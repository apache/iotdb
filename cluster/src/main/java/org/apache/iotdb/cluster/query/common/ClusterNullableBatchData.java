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
package org.apache.iotdb.cluster.query.common;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * <code>ClusterNullableBatchData</code> is a self-defined data structure which is used in cluster
 * query process of fill type and group by type, which may contain <code>null</code> in list of
 * TimeValuePair.
 */
public class ClusterNullableBatchData extends BatchData {

  private List<TimeValuePair> timeValuePairList;
  private int index;

  public ClusterNullableBatchData() {
    this.timeValuePairList = new ArrayList<>();
    this.index = 0;
  }

  @Override
  public boolean hasNext() {
    return index < timeValuePairList.size();
  }

  @Override
  public void next() {
    index++;
  }

  @Override
  public long currentTime() {
    rangeCheckForTime(index);
    return timeValuePairList.get(index).getTimestamp();
  }

  @Override
  public Object currentValue() {
    if (index < length()) {
      return timeValuePairList.get(index).getValue() == null ? null
          : timeValuePairList.get(index).getValue().getValue();
    } else {
      return null;
    }
  }

  @Override
  public int length() {
    return timeValuePairList.size();
  }

  public TimeValuePair getCurrentTimeValuePair() {
    return index < length() ? timeValuePairList.get(index) : null;
  }

  public void addTimeValuePair(TimeValuePair timeValuePair){
    timeValuePairList.add(timeValuePair);
  }
}
