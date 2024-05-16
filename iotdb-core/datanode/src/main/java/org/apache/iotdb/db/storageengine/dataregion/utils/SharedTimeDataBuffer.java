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

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SharedTimeDataBuffer {
  private ByteBuffer timeBuffer;
  private final IChunkMetadata timeChunkMetaData;
  private final List<Long[]> timeData;

  public SharedTimeDataBuffer(IChunkMetadata timeChunkMetaData) {
    this.timeChunkMetaData = timeChunkMetaData;
    this.timeData = new ArrayList<>();
  }

  // It should be called first before other methods in sharedTimeBuffer.
  public void init(TsFileSequenceReader reader) throws IOException {
    if (timeBuffer != null) {
      return;
    }
    Chunk timeChunk = reader.readMemChunk(timeChunkMetaData.getOffsetOfChunkHeader());
    timeBuffer = timeChunk.getData();
  }

  public synchronized Long[] getPageTime(int pageId) {
    int size = timeData.size();
    if (pageId < size) {
      return timeData.get(pageId);
    } else if (pageId == size) {
      loadPageData();
      return timeData.get(pageId);
    } else {
      throw new UnsupportedOperationException(
          "PageId in SharedTimeDataBuffer should be  incremental.");
    }
  }

  private void loadPageData() {
    if (timeBuffer.hasRemaining()) {
      int size = timeBuffer.getInt();
      Long[] pageData = new Long[size];
      for (int i = 0; i < size; i++) {
        pageData[i] = timeBuffer.getLong();
      }
      timeData.add(pageData);
    } else {
      throw new UnsupportedOperationException("No more data in SharedTimeDataBuffer");
    }
  }
}
