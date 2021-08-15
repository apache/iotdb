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
package org.apache.iotdb.tsfile.read.reader.series;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.List;

/**
 * Series reader is used to query one series of one TsFile, and this reader has a filter operating
 * on the same series.
 */
public class FileSeriesReader extends AbstractFileSeriesReader {

  public FileSeriesReader(
      IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList, Filter filter) {
    super(chunkLoader, chunkMetadataList, filter);
  }

  @Override
  protected void initChunkReader(IChunkMetadata chunkMetaData) throws IOException {
    Chunk chunk = chunkLoader.loadChunk((ChunkMetadata) chunkMetaData);
    this.chunkReader = new ChunkReader(chunk, filter);
  }

  @Override
  protected boolean chunkSatisfied(IChunkMetadata chunkMetaData) {
    return filter == null || filter.satisfy(chunkMetaData.getStatistics());
  }
}
