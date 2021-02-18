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
package org.apache.iotdb.db.query.reader.chunk;

import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;

import java.io.IOException;

/**
 * To read chunk data on disk by timestamp, this class implements an interface {@link
 * IReaderByTimestamp} based on the data reader {@link ChunkReaderByTimestamp}.
 *
 * <p>
 */
public class DiskChunkReaderByTimestamp implements IReaderByTimestamp {

  private ChunkReaderByTimestamp chunkReaderByTimestamp;
  private BatchData data;

  public DiskChunkReaderByTimestamp(ChunkReaderByTimestamp chunkReaderByTimestamp) {
    this.chunkReaderByTimestamp = chunkReaderByTimestamp;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {

    if (!hasNext()) {
      return null;
    }

    while (data != null) {
      Object value = data.getValueInTimestamp(timestamp);
      if (value != null) {
        return value;
      }
      if (data.hasCurrent()) {
        return null;
      } else {
        chunkReaderByTimestamp.setCurrentTimestamp(timestamp);
        if (chunkReaderByTimestamp.hasNextSatisfiedPage()) {
          data = chunkReaderByTimestamp.nextPageData();
        } else {
          return null;
        }
      }
    }

    return null;
  }

  private boolean hasNext() throws IOException {
    if (data != null && data.hasCurrent()) {
      return true;
    }
    if (chunkReaderByTimestamp != null && chunkReaderByTimestamp.hasNextSatisfiedPage()) {
      data = chunkReaderByTimestamp.nextPageData();
      return true;
    }
    return false;
  }
}
