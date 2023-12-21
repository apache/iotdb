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

import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;

public abstract class PageElement {

  private final ChunkMetadataElement chunkMetadataElement;

  private final boolean isLastPage;

  private final long priority;

  // pointReader is used to replace batchData to get rid of huge memory cost by loading data point
  // in a lazy way
  protected IPointReader pointReader;

  protected PageElement(
      ChunkMetadataElement chunkMetadataElement, boolean isLastPage, long priority) {
    this.chunkMetadataElement = chunkMetadataElement;
    this.isLastPage = isLastPage;
    this.priority = priority;
  }

  public abstract void deserializePage() throws IOException;

  public abstract long getStartTime();

  public abstract long getEndTime();

  public ChunkMetadataElement getChunkMetadataElement() {
    return chunkMetadataElement;
  }

  public boolean needForceDecoding() {
    return chunkMetadataElement.needForceDecoding;
  }

  public boolean isLastPage() {
    return isLastPage;
  }

  public long getPriority() {
    return priority;
  }

  public IPointReader getPointReader() {
    return pointReader;
  }
}
