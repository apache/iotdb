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

import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.Chunk;
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

  public List<PageHeader> valuePageHeaders;

  public TsBlock batchData;

  // pointReader is used to replace batchData to get rid of huge memory cost by loading data point
  // in a lazy way
  public IPointReader pointReader;

  // compressed page data
  public ByteBuffer pageData;

  public List<ByteBuffer> valuePageDatas;

  @SuppressWarnings("checkstyle:MemberNameCheck")
  public IChunkReader iChunkReader;

  public long priority;

  public long startTime;

  public boolean isSelected = false;

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
      List<PageHeader> valuePageHeaders,
      ByteBuffer timePageData,
      List<ByteBuffer> valuePageDatas,
      ChunkMetadataElement chunkMetadataElement,
      boolean isLastPage,
      long priority) {
    this.pageHeader = timePageHeader;
    this.valuePageHeaders = valuePageHeaders;
    this.pageData = timePageData;
    this.valuePageDatas = valuePageDatas;
    this.priority = priority;
    this.startTime = pageHeader.getStartTime();
    this.chunkMetadataElement = chunkMetadataElement;
    this.isLastPage = isLastPage;
    this.needForceDecoding = chunkMetadataElement.needForceDecoding;
  }

  public void deserializePage() throws IOException {
    if (this.iChunkReader == null) {
      List<ChunkHeader> valueChunkHeaderList =
          new ArrayList<>(chunkMetadataElement.valueChunks.size());
      List<List<TimeRange>> valueDeleteIntervalList =
          new ArrayList<>(chunkMetadataElement.valueChunks.size());
      for (Chunk valueChunk : chunkMetadataElement.valueChunks) {
        valueChunkHeaderList.add(valueChunk == null ? null : valueChunk.getHeader());
        valueDeleteIntervalList.add(valueChunk == null ? null : valueChunk.getDeleteIntervalList());
      }
      this.pointReader =
          AlignedChunkReader.getPagePointReader(
              chunkMetadataElement.chunk.getHeader(),
              valueChunkHeaderList,
              valueDeleteIntervalList,
              pageHeader,
              valuePageHeaders,
              pageData,
              valuePageDatas);
    } else {
      this.batchData = ((ChunkReader) iChunkReader).readPageData(pageHeader, pageData);
    }
  }
}
