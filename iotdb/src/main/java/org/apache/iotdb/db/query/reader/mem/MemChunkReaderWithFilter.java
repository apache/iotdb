/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.reader.mem;

import java.io.IOException;
import java.util.Iterator;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class MemChunkReaderWithFilter implements IReader {

  private Iterator<TimeValuePair> timeValuePairIterator;
  private Filter filter;
  private boolean hasCachedTimeValuePair;
  private TimeValuePair cachedTimeValuePair;

  public MemChunkReaderWithFilter(TimeValuePairSorter readableChunk, Filter filter) {
    timeValuePairIterator = readableChunk.getIterator();
    this.filter = filter;
  }

  @Override
  public boolean hasNext() {
    if (hasCachedTimeValuePair) {
      return true;
    }
    while (timeValuePairIterator.hasNext()) {
      TimeValuePair timeValuePair = timeValuePairIterator.next();
      if (filter.satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
        hasCachedTimeValuePair = true;
        cachedTimeValuePair = timeValuePair;
        break;
      }
    }
    return hasCachedTimeValuePair;
  }

  @Override
  public TimeValuePair next() {
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      return cachedTimeValuePair;
    } else {
      return timeValuePairIterator.next();
    }
  }

  @Override
  public void skipCurrentTimeValuePair() {
    next();
  }

  @Override
  public void close() throws IOException {
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
