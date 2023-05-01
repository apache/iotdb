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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class FileSpillerReader implements SortReader {

  FileInputStream fileInputStream;
  private final List<TsBlock> cacheBlocks;
  private int tsBlockIndex;
  private int rowIndex;
  private boolean isEnd = false;
  private final SortBufferManager sortBufferManager;

  public FileSpillerReader(String fileName, SortBufferManager sortBufferManager)
      throws FileNotFoundException {
    this.fileInputStream = new FileInputStream(fileName);
    this.cacheBlocks = new ArrayList<>();
    this.rowIndex = 0;
    this.sortBufferManager = sortBufferManager;
  }

  @Override
  public MergeSortKey next() {
    MergeSortKey sortKey = new MergeSortKey(cacheBlocks.get(tsBlockIndex), rowIndex);
    rowIndex++;
    return sortKey;
  }

  private boolean readTsBlockFromFile() throws IoTDBException {
    long bufferSize = sortBufferManager.getReaderBufferAvailable();
    cacheBlocks.clear();
    while (bufferSize >= DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES) {
      long size = read();
      if (size == -1) break;
      bufferSize -= size;
    }

    if (cacheBlocks.isEmpty()) {
      return false;
    }
    rowIndex = 0;
    tsBlockIndex = 0;
    return true;
  }

  private long read() throws IoTDBException {
    byte[] bytes = new byte[4];
    try {
      int readLen = fileInputStream.read(bytes);
      if (readLen == -1) {
        return -1;
      }
      int capacity = BytesUtils.bytesToInt(bytes);
      byte[] tsBlockBytes = ReadWriteIOUtils.readBytes(fileInputStream, capacity);
      ByteBuffer buffer = ByteBuffer.wrap(tsBlockBytes);
      TsBlock cachedTsBlock = TsBlockSerde.deserialize(buffer);
      cacheBlocks.add(cachedTsBlock);
      return cachedTsBlock.getRetainedSizeInBytes();
    } catch (IOException e) {
      throw new IoTDBException(e.getMessage(), TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  @Override
  public boolean hasNext() throws IoTDBException {

    if (isEnd) return false;

    if (cacheBlocks.isEmpty()
        || (rowIndex == cacheBlocks.get(tsBlockIndex).getPositionCount()
            && tsBlockIndex == cacheBlocks.size() - 1)) {
      boolean hasData = readTsBlockFromFile();
      if (!hasData) {
        isEnd = true;
        return false;
      }
      return true;
    }

    if (rowIndex < cacheBlocks.get(tsBlockIndex).getPositionCount()) {
      return true;
    }

    tsBlockIndex++;
    rowIndex = 0;
    return true;
  }

  @Override
  public void close() throws IoTDBException {
    try {
      fileInputStream.close();
    } catch (IOException e) {
      throw new IoTDBException(e.getMessage(), TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }
}
