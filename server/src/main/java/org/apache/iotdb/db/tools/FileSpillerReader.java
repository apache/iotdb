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

import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class FileSpillerReader implements SortReader {

  FileInputStream fileInputStream;
  TsBlock cacheBlock;
  int rowIndex;
  boolean isEnd = false;

  public FileSpillerReader(String fileName) throws FileNotFoundException {
    this.fileInputStream = new FileInputStream(fileName);
    this.cacheBlock = null;
    this.rowIndex = 0;
  }

  @Override
  public MergeSortKey next() {
    MergeSortKey sortKey = new MergeSortKey(cacheBlock, rowIndex);
    rowIndex++;
    return sortKey;
  }

  private boolean readTsBlockFromFile(FileInputStream fileInputStream) throws IOException {
    byte[] bytes = new byte[4];
    int readLen = fileInputStream.read(bytes);
    if (readLen == -1) {
      return false;
    }
    int capacity = BytesUtils.bytesToInt(bytes);
    byte[] tsBlockBytes = ReadWriteIOUtils.readBytes(fileInputStream, capacity);
    ByteBuffer buffer = ByteBuffer.wrap(tsBlockBytes);
    cacheBlock = TsBlockSerde.deserialize(buffer);
    rowIndex = 0;
    return true;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (cacheBlock == null || rowIndex == cacheBlock.getPositionCount()) {
      boolean hasData = readTsBlockFromFile(fileInputStream);
      if (!hasData) {
        isEnd = true;
        return false;
      }
    }
    return true;
  }
}
