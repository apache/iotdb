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
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AbstractChunkReader;

/**
 * To read chunk data on disk, this class implements an interface {@link IPointReader} based on the
 * data reader {@link AbstractChunkReader}.
 * <p>
 * Note that <code>ChunkReader</code> is an abstract class with three concrete classes, two of which
 * are used here: <code>ChunkReaderWithoutFilter</code> and <code>ChunkReaderWithFilter</code>.
 * <p>
 * This class is used in {@link org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceMergeReader}.
 */
public class DiskChunkReader implements IPointReader, IBatchReader {

  private AbstractChunkReader AbstractChunkReader;
  private BatchData data;

  public DiskChunkReader(AbstractChunkReader AbstractChunkReader) {
    this.AbstractChunkReader = AbstractChunkReader;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (data != null && data.hasCurrent()) {
      return true;
    }
    while (AbstractChunkReader.hasNextBatch()) {
      data = AbstractChunkReader.nextBatch();
      if (data.hasCurrent()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair next() {
    TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(data);
    data.next();
    return timeValuePair;
  }

  @Override
  public TimeValuePair current() {
    // FIXME: if data.hasNext() = false and this method is called...
    return TimeValuePairUtils.getCurrentTimeValuePair(data);
  }

  @Override
  public boolean hasNextBatch() throws IOException {
    return false;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return null;
  }

  @Override
  public void close() {
    this.AbstractChunkReader.close();
  }
}
