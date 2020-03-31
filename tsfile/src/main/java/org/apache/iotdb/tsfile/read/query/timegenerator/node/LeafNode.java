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
  private boolean hasCached;

  public LeafNode(IBatchReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasCached) {
      return true;
    }
    if (reader.hasNextBatch()) {
      hasCached = true;
      cacheData = reader.nextBatch();
    }
    return hasCached;
  }

  @Override
  public long next() throws IOException {
    if ((hasCached || hasNext())) {
      long currentTime = cacheData.currentTime();
      cacheData.next();
      hasCached = cacheData.hasCurrent();
      return currentTime;
    }
    throw new IOException("no more data");
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
