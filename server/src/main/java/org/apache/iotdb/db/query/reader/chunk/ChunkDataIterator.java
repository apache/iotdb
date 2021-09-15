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

import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;

/**
 * To read chunk data on disk, this class implements an interface {@link IPointReader} based on the
 * data reader {@link ChunkReader}.
 *
 * <p>Note that <code>ChunkReader</code> is an abstract class with three concrete classes, two of
 * which are used here: <code>ChunkReaderWithoutFilter</code> and <code>ChunkReaderWithFilter</code>
 * .
 *
 * <p>
 */
public class ChunkDataIterator implements IPointReader {

  private IChunkReader chunkReader;
  private BatchData data;

  public ChunkDataIterator(IChunkReader chunkReader) {
    this.chunkReader = chunkReader;
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    if (data != null && data.hasCurrent()) {
      return true;
    }
    while (chunkReader.hasNextSatisfiedPage()) {
      data = chunkReader.nextPageData();
      if (data.hasCurrent()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(data);
    data.next();
    return timeValuePair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    return TimeValuePairUtils.getCurrentTimeValuePair(data);
  }

  @Override
  public void close() throws IOException {
    this.chunkReader.close();
  }
}
