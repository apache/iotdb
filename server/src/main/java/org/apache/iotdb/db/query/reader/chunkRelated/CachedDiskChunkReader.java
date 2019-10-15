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

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

public class CachedDiskChunkReader implements IPointReader {

  private ChunkReader chunkReader;
  private BatchData data;
  private TimeValuePair prev;
  private TimeValuePair current;

  public CachedDiskChunkReader(ChunkReader chunkReader) {
    this.chunkReader = chunkReader;
    this.prev =
        TimeValuePairUtils.getEmptyTimeValuePair(chunkReader.getChunkHeader().getDataType());
  }

  @Override
  public boolean hasNext() throws IOException {
    if (data != null && data.hasNext()) {
      return true;
    }
    while (chunkReader.hasNextBatch()) {
      data = chunkReader.nextBatch();
      if (data.hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    TimeValuePairUtils.setCurrentTimeValuePair(data, prev);
    data.next();
    if (data.hasNext()) {
      TimeValuePairUtils.setCurrentTimeValuePair(data, current());
    } else {
      while (chunkReader.hasNextBatch()) {
        data = chunkReader.nextBatch();
        if (data.hasNext()) {
          TimeValuePairUtils.setCurrentTimeValuePair(data, current());
          break;
        }
      }
    }
    return prev;
  }

  @Override
  public TimeValuePair current() {
    if (current == null) {
      this.current =
          TimeValuePairUtils.getEmptyTimeValuePair(chunkReader.getChunkHeader().getDataType());
      TimeValuePairUtils.setCurrentTimeValuePair(data, current);
    }
    return current;
  }

  @Override
  public void close() {
    this.chunkReader.close();
  }
}
