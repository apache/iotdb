/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.reader.unsequence;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;

public class EngineChunkReaderByTimestamp implements EngineReaderByTimeStamp {

  private ChunkReaderByTimestamp chunkReader;
  private BatchData data;

  /**
   * Each EngineChunkReader has a corresponding UnClosedTsFileReader, when EngineChunkReader is
   * closed, UnClosedTsFileReader also should be closed in meanwhile.
   */
  private TsFileSequenceReader unClosedTsFileReader;

  public EngineChunkReaderByTimestamp(ChunkReaderByTimestamp chunkReader,
      TsFileSequenceReader unClosedTsFileReader) {
    this.chunkReader = chunkReader;
    this.unClosedTsFileReader = unClosedTsFileReader;
  }

  /**
   * get value with time equals timestamp. If there is no such point, return null.
   */
  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {

    while (data != null) {
      Object value = data.getValueInTimestamp(timestamp);
      if (value != null) {
        return value;
      }
      if (data.hasNext()) {
        return null;
      } else {
        chunkReader.setCurrentTimestamp(timestamp);
        if (chunkReader.hasNextBatch()) {
          data = chunkReader.nextBatch();
        } else {
          return null;
        }
      }
    }

    return null;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (data != null & data.hasNext()) {
      return true;
    }
    if (chunkReader != null && chunkReader.hasNextBatch()) {
      data = chunkReader.nextBatch();
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    this.chunkReader.close();
    this.unClosedTsFileReader.close();
  }
}
