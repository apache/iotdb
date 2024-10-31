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

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SharedTimeDataBuffer {
  private ByteBuffer timeBuffer;
  private final IChunkMetadata timeChunkMetaData;
  private ChunkHeader timeChunkHeader;
  private final List<long[]> timeData;
  private final Decoder defaultTimeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  private EncryptParameter encryptParam;

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
    timeChunkHeader = timeChunk.getHeader();
    timeBuffer = timeChunk.getData();
    encryptParam = timeChunk.getEncryptParam();
  }

  public long[] getPageTime(int pageId) throws IOException {
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

  private void loadPageData() throws IOException {
    if (!timeBuffer.hasRemaining()) {
      throw new UnsupportedOperationException("No more data in SharedTimeDataBuffer");
    }
    PageHeader timePageHeader =
        isSinglePageChunk()
            ? PageHeader.deserializeFrom(timeBuffer, timeChunkMetaData.getStatistics())
            : PageHeader.deserializeFrom(timeBuffer, timeChunkHeader.getDataType());
    IDecryptor decryptor = IDecryptor.getDecryptor(encryptParam);
    ByteBuffer timePageData =
        ChunkReader.deserializePageData(timePageHeader, timeBuffer, timeChunkHeader, decryptor);
    long[] pageData = new long[(int) timePageHeader.getNumOfValues()];
    int index = 0;
    while (defaultTimeDecoder.hasNext(timePageData)) {
      pageData[index++] = defaultTimeDecoder.readLong(timePageData);
    }
    timeData.add(pageData);
  }

  private boolean isSinglePageChunk() {
    return (this.timeChunkHeader.getChunkType() & 63) == 5;
  }
}
