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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.reader.CompactionChunkReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NonAlignedPageElement extends PageElement {

  private final PageHeader pageHeader;

  // compressed page data
  private final ByteBuffer pageData;

  private final CompactionChunkReader chunkReader;

  public NonAlignedPageElement(
      PageHeader pageHeader,
      ByteBuffer pageData,
      CompactionChunkReader chunkReader,
      ChunkMetadataElement chunkMetadataElement,
      boolean isLastPage,
      long priority) {
    super(chunkMetadataElement, isLastPage, priority);
    this.pageHeader = pageHeader;
    this.pageData = pageData;
    this.chunkReader = chunkReader;
  }

  @Override
  public void deserializePage() throws IOException {
    TsBlock batchData = chunkReader.readPageData(pageHeader, pageData);
    this.pointReader = batchData.getTsBlockSingleColumnIterator();
  }

  @Override
  public long getStartTime() {
    return pageHeader.getStartTime();
  }

  @Override
  public long getEndTime() {
    return pageHeader.getEndTime();
  }

  public PageHeader getPageHeader() {
    return pageHeader;
  }

  public ByteBuffer getPageData() {
    return pageData;
  }
}
