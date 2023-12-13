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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.reader.CompactionAlignedChunkReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class AlignedPageElement extends PageElement {

  private final PageHeader timePageHeader;

  private final List<PageHeader> valuePageHeaders;

  // compressed page data
  private final ByteBuffer timePageData;

  private final List<ByteBuffer> valuePageDataList;

  private final CompactionAlignedChunkReader chunkReader;

  @SuppressWarnings("squid:S107")
  public AlignedPageElement(
      PageHeader timePageHeader,
      List<PageHeader> valuePageHeaders,
      ByteBuffer timePageData,
      List<ByteBuffer> valuePageDataList,
      CompactionAlignedChunkReader alignedChunkReader,
      ChunkMetadataElement chunkMetadataElement,
      boolean isLastPage,
      long priority) {
    super(chunkMetadataElement, isLastPage, priority);
    this.timePageHeader = timePageHeader;
    this.valuePageHeaders = valuePageHeaders;
    this.timePageData = timePageData;
    this.valuePageDataList = valuePageDataList;
    this.chunkReader = alignedChunkReader;
  }

  @Override
  public void deserializePage() throws IOException {
    // For aligned page, we use pointReader rather than deserialize all data point to get rid of
    // huge memory cost
    pointReader =
        chunkReader.getPagePointReader(
            timePageHeader, valuePageHeaders, timePageData, valuePageDataList);
  }

  @Override
  public long getStartTime() {
    return timePageHeader.getStartTime();
  }

  @Override
  public long getEndTime() {
    return timePageHeader.getEndTime();
  }

  public PageHeader getTimePageHeader() {
    return timePageHeader;
  }

  public List<PageHeader> getValuePageHeaders() {
    return valuePageHeaders;
  }

  public ByteBuffer getTimePageData() {
    return timePageData;
  }

  public List<ByteBuffer> getValuePageDataList() {
    return valuePageDataList;
  }
}
