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

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * Series reader is used to query one series of one tsfile.
 */
public class FileSeriesPageReader extends FileSeriesReader {

  private BatchData data;

  /**
   * constructor of FileSeriesReader.
   */
  public FileSeriesPageReader(IChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList,
      Filter filter) {
    super(chunkLoader, chunkMetaDataList, filter);
    this.chunkToRead = 0;
  }

  public FileSeriesPageReader(IChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
    super(chunkLoader, chunkMetaDataList);
    this.chunkToRead = 0;
  }

  @Override
  public boolean hasNext() throws IOException {
    // current chunk has additional batch
    if (chunkReader != null && chunkReader.hasNextBatch()) {
      return true;
    }

    // current chunk does not have additional batch, init new chunk reader
    while (chunkToRead < chunkMetaDataList.size()) {

      ChunkMetaData chunkMetaData = nextChunkMeta();
      if (chunkSatisfied(chunkMetaData)) {
        // chunk metadata satisfy the condition
        initChunkReader(chunkMetaData);

        if (chunkReader.hasNextBatch()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public <T> T nextHeader() throws IOException {
    return (T) chunkReader.nextPageHeader();
  }

  @Override
  public <T> T nextData() throws IOException {
    data = chunkReader.nextBatch();
    return (T) data;
  }

  @Override
  public void skipData() {
    chunkReader.skipPageData();
  }

  public BatchData currentBatch() {
    return data;
  }
}
