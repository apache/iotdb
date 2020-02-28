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
package org.apache.iotdb.tsfile.read.query.timegenerator.node;

import java.io.IOException;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.TimeColumn;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

public class LeafNode implements Node {

  private IBatchReader reader;

  private BatchData cacheData;
  private TimeColumn cachedTimeSeries;
  private boolean hasCached;

  public LeafNode(IBatchReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNextTimeColumn() throws IOException {
    if (hasCached) {
      return true;
    }
    while (reader.hasNextBatch()) {
      cacheData = reader.nextBatch();
      if (cacheData.hasCurrent()) {
        hasCached = true;
        cachedTimeSeries = cacheData.getTimeColumn();
        break;
      }
    }
    return hasCached;
  }

  @Override
  public TimeColumn nextTimeColumn() throws IOException {
    if (hasCached || hasNextTimeColumn()) {
      hasCached = false;
      return cachedTimeSeries;
    }
    throw new IOException("no more data");
  }

  /**
   * Check whether the current time equals the given time.
   *
   * @param time the given time
   * @return True if the current time equals the given time. False if not.
   */
  public boolean currentTimeIs(long time) {
    if (!cacheData.hasCurrent()) {
      return false;
    }
    return cacheData.currentTime() == time;
  }

  /**
   * Function for getting the value at the given time.
   */
  public Object currentValue(long time) {
    return cacheData.getValueInTimestamp(time);
  }

  @Override
  public NodeType getType() {
    return NodeType.LEAF;
  }

}