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
package org.apache.iotdb.db.query.reader.unsequence;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

public class EngineChunkReader implements IReader {

  private ChunkReader chunkReader;
  private BatchData data;

  /**
   * Each EngineChunkReader has a corresponding UnClosedTsFileReader, when EngineChunkReader is
   * closed, UnClosedTsFileReader also should be closed in meanwhile.
   */
  private TsFileSequenceReader unClosedTsFileReader;

  public EngineChunkReader(ChunkReader chunkReader, TsFileSequenceReader unClosedTsFileReader) {
    this.chunkReader = chunkReader;
    this.unClosedTsFileReader = unClosedTsFileReader;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (data == null || !data.hasNext()) {
      if (chunkReader.hasNextBatch()) {
        data = chunkReader.nextBatch();
      } else {
        return false;
      }
    }

    return data.hasNext();
  }

  @Override
  public TimeValuePair next() {
    TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(data);
    data.next();
    return timeValuePair;
  }

  @Override
  public void skipCurrentTimeValuePair() {
    next();
  }

  @Override
  public void close() throws IOException {
    this.chunkReader.close();
    this.unClosedTsFileReader.close();
  }

  // TODO
  @Override
  public boolean hasNextBatch() {
    return false;
  }

  // TODO
  @Override
  public BatchData nextBatch() {
    return null;
  }

  // TODO
  @Override
  public BatchData currentBatch() {
    return null;
  }
}
