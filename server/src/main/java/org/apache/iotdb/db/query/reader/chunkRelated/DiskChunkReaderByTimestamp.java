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
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;

/**
 * To read chunk data on disk by timestamp, this class implements an interface {@link
 * IReaderByTimestamp} based on the data reader {@link ChunkReaderByTimestamp}.
 * <p>
 * This class is used in {@link org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceReaderByTimestamp}.
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
        if (chunkReaderByTimestamp.hasNextBatch()) {
          data = chunkReaderByTimestamp.nextBatch();
        } else {
          return null;
        }
      }
    }

    return null;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (data != null && data.hasCurrent()) {
      return true;
    }
    if (chunkReaderByTimestamp != null && chunkReaderByTimestamp.hasNextBatch()) {
      data = chunkReaderByTimestamp.nextBatch();
      return true;
    }
    return false;
  }
}