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

import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * <code>FillBatchData</code> is a self-defined data structure which is used in cluster query
 * process of fill type, which only contains one TimeValuePair and value can be null.
 */
public class FillBatchData extends BatchData {

  private TimeValuePair timeValuePair;
  private boolean isUsed;

  public FillBatchData(TimeValuePair timeValuePair, boolean isUsed) {
    this.timeValuePair = timeValuePair;
    this.isUsed = isUsed;
  }

  @Override
  public boolean hasNext() {
    return !isUsed;
  }

  @Override
  public void next() {
    isUsed = true;
  }

  @Override
  public long currentTime() {
    return timeValuePair.getTimestamp();
  }

  @Override
  public Object currentValue() {
    if (!isUsed) {
      return timeValuePair.getValue() == null ? null : timeValuePair.getValue().getValue();
    } else {
      return null;
    }
  }

  public TimeValuePair getTimeValuePair() {
    return isUsed ? null : timeValuePair;
  }
}
