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
package org.apache.iotdb.db.query.reader.chunkRelated;

import java.util.Iterator;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.fileRelated.UnSealedTsFileReaderByTimestamp;
import org.apache.iotdb.db.utils.TimeValuePair;

/**
 * To read data in memory by timestamp, this class implements an interface {@link
 * IReaderByTimestamp} based on the data source {@link ReadOnlyMemChunk}.
 * <p>
 * This class is used in {@link UnSealedTsFileReaderByTimestamp} and {@link
 * org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceReaderByTimestamp}.
 */
public class MemChunkReaderByTimestamp implements IReaderByTimestamp {

  private Iterator<TimeValuePair> timeValuePairIterator;
  private boolean hasCachedTimeValuePair;
  private TimeValuePair cachedTimeValuePair;

  public MemChunkReaderByTimestamp(ReadOnlyMemChunk readableChunk) {
    timeValuePairIterator = readableChunk.getIterator();
  }

  @Override
  public boolean hasNext() {
    if (hasCachedTimeValuePair) {
      return true;
    }
    return timeValuePairIterator.hasNext();
  }

  private TimeValuePair next() {
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      return cachedTimeValuePair;
    } else {
      return timeValuePairIterator.next();
    }
  }

  // TODO consider change timeValuePairIterator to List structure, and use binary search instead of
  // sequential search
  @Override
  public Object getValueInTimestamp(long timestamp) {
    while (hasNext()) {
      TimeValuePair timeValuePair = next();
      long time = timeValuePair.getTimestamp();
      if (time == timestamp) {
        return timeValuePair.getValue().getValue();
      } else if (time > timestamp) {
        hasCachedTimeValuePair = true;
        cachedTimeValuePair = timeValuePair;
        break;
      }
    }
    return null;
  }
}
