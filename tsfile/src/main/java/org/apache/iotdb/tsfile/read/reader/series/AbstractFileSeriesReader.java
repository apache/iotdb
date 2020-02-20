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

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IAggregateReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.List;

/**
 * Series reader is used to query one series of one tsfile.
 */
public abstract class AbstractFileSeriesReader implements IAggregateReader {

  protected IChunkLoader chunkLoader;
  protected List<ChunkMetaData> chunkMetaDataList;
  protected ChunkReader chunkReader;
  private int chunkToRead;

  private BatchData data;

  protected Filter filter;

  /**
   * constructor of FileSeriesReader.
   */
  public AbstractFileSeriesReader(IChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList,
      Filter filter) {
    this.chunkLoader = chunkLoader;
    this.chunkMetaDataList = chunkMetaDataList;
    this.filter = filter;
    this.chunkToRead = 0;
  }

  /**
   * check if current chunk has next batch data.
   *
   * @return True if current chunk has next batch data
   */
  public boolean hasNextBatch() throws IOException {

    // current chunk has additional batch
    if (chunkReader != null && chunkReader.hasNextSatisfiedPage()) {
      return true;
    }

    // current chunk does not have additional batch, init new chunk reader
    while (chunkToRead < chunkMetaDataList.size()) {

      ChunkMetaData chunkMetaData = nextChunkMeta();
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

  /**
   * get next batch data.
   */
  public BatchData nextBatch() throws IOException {
    data = chunkReader.nextPageData();
    return data;
  }

  public BatchData currentBatch() {
    return data;
  }

  public PageHeader nextPageHeader() {
    return chunkReader.nextPageHeader();
  }

  public void skipPageData() {
    chunkReader.skipPageData();
  }

  protected abstract void initChunkReader(ChunkMetaData chunkMetaData) throws IOException;

  protected abstract boolean chunkSatisfied(ChunkMetaData chunkMetaData);

  public void close() throws IOException {
    chunkLoader.close();
  }

  private ChunkMetaData nextChunkMeta() {
    return chunkMetaDataList.get(chunkToRead++);
  }
}
