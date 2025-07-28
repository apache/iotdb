/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl;

import org.apache.iotdb.db.storageengine.dataregion.utils.SharedTimeDataBuffer;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class DiskAlignedChunkHandleImpl extends DiskChunkHandleImpl {
  private static final int MASK = 0x80;

  private final SharedTimeDataBuffer sharedTimeDataBuffer;
  private int pageIndex = 0;

  private EncryptParameter encryptParam;

  public DiskAlignedChunkHandleImpl(
      IDeviceID deviceID,
      String measurement,
      String filePath,
      boolean isTsFileClosed,
      long offset,
      Statistics<? extends Serializable> chunkStatistic,
      SharedTimeDataBuffer sharedTimeDataBuffer) {
    super(deviceID, measurement, filePath, isTsFileClosed, offset, chunkStatistic);
    this.sharedTimeDataBuffer = sharedTimeDataBuffer;
  }

  @Override
  protected void init(TsFileSequenceReader reader) throws IOException {
    sharedTimeDataBuffer.init(reader);
    super.init(reader);
    this.encryptParam = reader.getEncryptParam();
  }

  @Override
  public long[] getDataTime() throws IOException {
    IDecryptor decryptor = IDecryptor.getDecryptor(encryptParam);
    ByteBuffer currentPageDataBuffer =
        ChunkReader.deserializePageData(
            this.currentPageHeader,
            this.currentChunkDataBuffer,
            this.currentChunkHeader,
            decryptor);
    int size = ReadWriteIOUtils.readInt(currentPageDataBuffer);
    byte[] bitmap = new byte[(size + 7) / 8];
    currentPageDataBuffer.get(bitmap);

    long[] timeData = sharedTimeDataBuffer.getPageTime(pageIndex);
    if (timeData.length != size) {
      throw new UnsupportedOperationException("Time data size not match");
    }

    long[] validTimeList = new long[(int) this.currentPageHeader.getNumOfValues()];
    int index = 0;
    for (int i = 0; i < timeData.length; i++) {
      if ((bitmap[i / 8] & 0xFF & MASK >>> i % 8) == 0) {
        continue;
      }
      validTimeList[index++] = timeData[i];
    }

    pageIndex++;
    return validTimeList;
  }
}
