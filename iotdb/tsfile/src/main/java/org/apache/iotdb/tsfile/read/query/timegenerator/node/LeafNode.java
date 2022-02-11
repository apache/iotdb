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

import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;

public class LeafNode implements Node {

  private IBatchReader reader;

  private BatchData cacheData;
  private boolean hasCached;

  private long cachedTime;
  private Object cachedValue;

  public LeafNode(IBatchReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasCached) {
      return true;
    }
    if (cacheData != null && cacheData.hasCurrent()) {
      cachedTime = cacheData.currentTime();
      cachedValue = cacheData.currentValue();
      hasCached = true;
      return true;
    }
    while (reader.hasNextBatch()) {
      cacheData = reader.nextBatch();
      if (cacheData.hasCurrent()) {
        cachedTime = cacheData.currentTime();
        cachedValue = cacheData.currentValue();
        hasCached = true;
        return true;
      }
    }
    return false;
  }

  @Override
  public long next() throws IOException {
    if ((hasCached || hasNext())) {
      hasCached = false;
      cacheData.next();
      return cachedTime;
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
    return cachedTime == time;
  }

  /** Function for getting the value at the given time. */
  public Object currentValue() {
    return cachedValue;
  }

  @Override
  public NodeType getType() {
    return NodeType.LEAF;
  }
}
