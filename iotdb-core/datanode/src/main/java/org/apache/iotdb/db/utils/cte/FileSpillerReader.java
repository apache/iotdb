/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.utils.cte;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class FileSpillerReader implements CteDataReader {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(FileSpillerReader.class);

  private final FileChannel fileChannel;
  private final List<TsBlock> cachedData;
  private final String fileName;
  private final TsBlockSerde serde;

  private int tsBlockIndex;
  private boolean isEnd = false;

  private long cachedBytes = 0;

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  public FileSpillerReader(String fileName, TsBlockSerde serde) throws IOException {
    this.fileName = fileName;
    this.serde = serde;
    this.tsBlockIndex = 0;
    this.fileChannel = FileChannel.open(Paths.get(fileName), StandardOpenOption.READ);
    this.cachedData = new ArrayList<>();
  }

  @Override
  public TsBlock next() throws IoTDBException {
    if (cachedData == null || tsBlockIndex >= cachedData.size()) {
      return null;
    }
    return cachedData.get(tsBlockIndex++);
  }

  @Override
  public boolean hasNext() throws IoTDBException {
    if (isEnd) {
      return false;
    }

    if (cachedData.isEmpty() || tsBlockIndex == cachedData.size() - 1) {
      boolean hasData = readTsBlockFromFile();
      if (!hasData) {
        isEnd = true;
        cachedData.clear();
        cachedBytes = 0L;
        return false;
      }
      return true;
    }

    return tsBlockIndex < cachedData.size();
  }

  @Override
  public void close() throws IoTDBException {
    try {
      fileChannel.close();
    } catch (IOException e) {
      throw new IoTDBException(
          "Can't close FileSpillerReader: " + fileName,
          e,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  @Override
  public long bytesUsed() {
    return INSTANCE_SIZE + cachedBytes;
  }

  private boolean readTsBlockFromFile() throws IoTDBException {
    long cteBufferSize = IoTDBDescriptor.getInstance().getConfig().getCteBufferSize();
    cachedData.clear();
    cachedBytes = 0L;
    while (cteBufferSize >= DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES) {
      long size = read();
      if (size == -1) {
        break;
      }
      cteBufferSize -= size;
      cachedBytes += size;
    }

    tsBlockIndex = 0;
    return !cachedData.isEmpty();
  }

  private long read() throws IoTDBException {
    try {
      ByteBuffer bytes = ByteBuffer.allocate(4);
      int readLen = fileChannel.read(bytes);
      if (readLen == -1) {
        return -1;
      }
      bytes.flip();
      int capacity = bytes.getInt();
      ByteBuffer tsBlockBytes = ByteBuffer.allocate(capacity);
      fileChannel.read(tsBlockBytes);
      tsBlockBytes.flip();
      TsBlock cachedTsBlock = serde.deserialize(tsBlockBytes);
      cachedData.add(cachedTsBlock);
      return cachedTsBlock.getRetainedSizeInBytes();
    } catch (IOException e) {
      throw new IoTDBException(
          "Can't read a new tsBlock in FileSpillerReader: " + fileName,
          e,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }
}
