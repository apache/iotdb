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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.LazyChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.LazyPageLoader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("squid:S1104")
public class PageElement {

  public PageHeader pageHeader;

  public TsBlock batchData;

  // pointReader is used to replace batchData to get rid of huge memory cost by loading data point
  // in a lazy way
  public IPointReader pointReader;

  public ByteBuffer pageData;

  public LazyPageLoader timePageLoader;
  public List<LazyPageLoader> valuePageLoaders;

  @SuppressWarnings("checkstyle:MemberNameCheck")
  public IChunkReader iChunkReader;

  public long priority;

  public long startTime;

  public boolean isLastPage;

  public ChunkMetadataElement chunkMetadataElement;

  public boolean needForceDecoding;

  public PageElement(
      PageHeader pageHeader,
      ByteBuffer pageData,
      ChunkReader chunkReader,
      ChunkMetadataElement chunkMetadataElement,
      boolean isLastPage,
      long priority) {
    this.pageHeader = pageHeader;
    this.pageData = pageData;
    this.priority = priority;
    this.iChunkReader = chunkReader;
    this.startTime = pageHeader.getStartTime();
    this.chunkMetadataElement = chunkMetadataElement;
    this.isLastPage = isLastPage;
    this.needForceDecoding = chunkMetadataElement.needForceDecoding;
  }

  @SuppressWarnings("squid:S107")
  public PageElement(
      PageHeader timePageHeader,
      LazyPageLoader timePageLoader,
      List<LazyPageLoader> valuePageLoaders,
      ChunkMetadataElement chunkMetadataElement,
      boolean isLastPage,
      long priority) {
    this.pageHeader = timePageHeader;
    this.timePageLoader = timePageLoader;
    this.valuePageLoaders = valuePageLoaders;
    this.priority = priority;
    this.startTime = pageHeader.getStartTime();
    this.chunkMetadataElement = chunkMetadataElement;
    this.isLastPage = isLastPage;
    this.needForceDecoding = chunkMetadataElement.needForceDecoding;
  }

  public void deserializePage() throws IOException {
    if (timePageLoader != null) {
      ChunkHeader timeChunkHeader = chunkMetadataElement.timeChunkLoader.loadChunkHeader();
      List<ChunkHeader> valueChunkHeaderList =
          new ArrayList<>(chunkMetadataElement.valueChunkLoaders.size());
      List<List<TimeRange>> valueDeleteIntervalList =
          new ArrayList<>(chunkMetadataElement.valueChunkLoaders.size());
      for (LazyChunkLoader valueChunkLoader : chunkMetadataElement.valueChunkLoaders) {
        valueChunkHeaderList.add(valueChunkLoader.loadChunkHeader());
        valueDeleteIntervalList.add(valueChunkLoader.getChunkMetadata().getDeleteIntervalList());
      }
      List<PageHeader> valuePageHeaders = new ArrayList<>();
      List<ByteBuffer> valuePageDatas = new ArrayList<>();
      for (LazyPageLoader valuePageLoader : valuePageLoaders) {
        valuePageHeaders.add(valuePageLoader.getPageHeader());
        valuePageDatas.add(valuePageLoader.loadPage());
      }
      this.pointReader =
          AlignedChunkReader.getPagePointReader(
              timeChunkHeader,
              valueChunkHeaderList,
              valueDeleteIntervalList,
              timePageLoader.getPageHeader(),
              valuePageHeaders,
              timePageLoader.loadPage(),
              valuePageDatas);
    } else {
      this.batchData = ((ChunkReader) iChunkReader).readPageData(pageHeader, pageData);
    }
  }
}
