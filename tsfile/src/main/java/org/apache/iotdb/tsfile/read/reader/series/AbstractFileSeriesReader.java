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

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.List;

/** Series reader is used to query one series of one tsfile. */
public abstract class AbstractFileSeriesReader implements IBatchReader {

  protected IChunkLoader chunkLoader;
  protected List<IChunkMetadata> chunkMetadataList;
  protected ChunkReader chunkReader;
  private int chunkToRead;

  protected Filter filter;

  /** constructor of FileSeriesReader. */
  public AbstractFileSeriesReader(
      IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList, Filter filter) {
    this.chunkLoader = chunkLoader;
    this.chunkMetadataList = chunkMetadataList;
    this.filter = filter;
    this.chunkToRead = 0;
  }

  @Override
  public boolean hasNextBatch() throws IOException {

    // current chunk has additional batch
    if (chunkReader != null && chunkReader.hasNextSatisfiedPage()) {
      return true;
    }

    // current chunk does not have additional batch, init new chunk reader
    while (chunkToRead < chunkMetadataList.size()) {

      IChunkMetadata chunkMetaData = nextChunkMeta();
      if (chunkSatisfied(chunkMetaData)) {
        // chunk metadata satisfy the condition
        initChunkReader(chunkMetaData);

        if (chunkReader.hasNextSatisfiedPage()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return chunkReader.nextPageData();
  }

  protected abstract void initChunkReader(IChunkMetadata chunkMetaData) throws IOException;

  protected abstract boolean chunkSatisfied(IChunkMetadata chunkMetaData);

  @Override
  public void close() throws IOException {
    chunkLoader.close();
  }

  private IChunkMetadata nextChunkMeta() {
    return chunkMetadataList.get(chunkToRead++);
  }
}
