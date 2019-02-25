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
package org.apache.iotdb.db.query.reader.mem;

import java.io.IOException;
import java.util.Iterator;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MemChunkReaderByTimestamp implements EngineReaderByTimeStamp {

  private Iterator<TimeValuePair> timeValuePairIterator;
  private boolean hasCachedTimeValuePair;
  private TimeValuePair cachedTimeValuePair;

  public MemChunkReaderByTimestamp(TimeValuePairSorter readableChunk) {
    timeValuePairIterator = readableChunk.getIterator();
  }

  @Override
  public boolean hasNext() {
    if (hasCachedTimeValuePair) {
      return true;
    }
    return timeValuePairIterator.hasNext();
  }

  @Override
  public TimeValuePair next() throws IOException {
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      return cachedTimeValuePair;
    } else {
      return timeValuePairIterator.next();
    }
  }

  @Override
  public void skipCurrentTimeValuePair() throws IOException {
    next();
  }

  @Override
  public void close() {
    // Do nothing because mem chunk reader will not open files
  }

  // TODO consider change timeValuePairIterator to List structure, and use binary search instead of
  // sequential search
  @Override
  public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
    while (hasNext()) {
      TimeValuePair timeValuePair = next();
      long time = timeValuePair.getTimestamp();
      if (time == timestamp) {
        return timeValuePair.getValue();
      } else if (time > timestamp) {
        hasCachedTimeValuePair = true;
        cachedTimeValuePair = timeValuePair;
        break;
      }
    }
    return null;
  }

  @Override
  public boolean hasNextBatch() {
    return false;
  }

  @Override
  public BatchData nextBatch() {
    return null;
  }

  @Override
  public BatchData currentBatch() {
    return null;
  }

}
