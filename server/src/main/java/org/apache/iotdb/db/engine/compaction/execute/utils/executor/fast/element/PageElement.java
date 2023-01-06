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
package org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast.element;

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class PageElement {

  public PageHeader pageHeader;

  public List<PageHeader> valuePageHeaders;

  public TsBlock batchData;

  // compressed page data
  public ByteBuffer pageData;

  public List<ByteBuffer> valuePageDatas;

  public IChunkReader iChunkReader;

  public long priority;

  public long startTime;

  public boolean isSelected = false;

  public boolean isLastPage;

  public ChunkMetadataElement chunkMetadataElement;

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
  }

  public PageElement(
      PageHeader timePageHeader,
      List<PageHeader> valuePageHeaders,
      ByteBuffer timePageData,
      List<ByteBuffer> valuePageDatas,
      AlignedChunkReader alignedChunkReader,
      ChunkMetadataElement chunkMetadataElement,
      boolean isLastPage,
      long priority) {
    this.pageHeader = timePageHeader;
    this.valuePageHeaders = valuePageHeaders;
    this.pageData = timePageData;
    this.valuePageDatas = valuePageDatas;
    this.priority = priority;
    this.iChunkReader = alignedChunkReader;
    this.startTime = pageHeader.getStartTime();
    this.chunkMetadataElement = chunkMetadataElement;
    this.isLastPage = isLastPage;
  }

  public void deserializePage() throws IOException {
    if (iChunkReader instanceof AlignedChunkReader) {
      this.batchData =
          ((AlignedChunkReader) iChunkReader)
              .readPageData(pageHeader, valuePageHeaders, pageData, valuePageDatas);
    } else {
      this.batchData = ((ChunkReader) iChunkReader).readPageData(pageHeader, pageData);
    }
  }
}
