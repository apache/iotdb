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

package org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.TsFileTableDiskUsageCacheWriter.KEY_FILE_RECORD_TYPE_OFFSET;
import static org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.TsFileTableDiskUsageCacheWriter.KEY_FILE_RECORD_TYPE_REDIRECT;

public class TsFileTableSizeCacheWriter {
  private final int regionId;
  private final File currentKeyIndexFile;
  private final File currentValueIndexFile;
  private FileOutputStream keyFileOutputStream;
  private FileOutputStream valueFileOutputStream;
  private BufferedOutputStream keyBufferedOutputStream;
  private BufferedOutputStream valueBufferedOutputStream;
  private long keyFileSize;
  private long valueFileSize;

  public TsFileTableSizeCacheWriter(
      int regionId, File currentKeyIndexFile, File currentValueIndexFile) throws IOException {
    this(regionId, currentKeyIndexFile, currentValueIndexFile, true);
  }

  public TsFileTableSizeCacheWriter(
      int regionId, File currentKeyIndexFile, File currentValueIndexFile, boolean recover)
      throws IOException {
    this.regionId = regionId;
    this.currentKeyIndexFile = currentKeyIndexFile;
    this.currentValueIndexFile = currentValueIndexFile;
    currentKeyIndexFile.createNewFile();
    currentValueIndexFile.createNewFile();
    if (recover) {
      recover();
    } else {
      keyFileSize = currentKeyIndexFile.length();
      valueFileSize = currentValueIndexFile.length();
    }
  }

  private void recover() throws IOException {
    TsFileTableSizeCacheReader cacheFileReader =
        new TsFileTableSizeCacheReader(
            currentKeyIndexFile.length(),
            currentKeyIndexFile,
            currentValueIndexFile.length(),
            currentValueIndexFile,
            regionId);
    Pair<Long, Long> truncateSize = cacheFileReader.selfCheck();
    if (truncateSize.left != currentKeyIndexFile.length()) {
      try (FileChannel channel =
          FileChannel.open(currentKeyIndexFile.toPath(), StandardOpenOption.WRITE)) {
        channel.truncate(truncateSize.left);
      }
    }
    if (truncateSize.right != currentValueIndexFile.length()) {
      try (FileChannel channel =
          FileChannel.open(currentValueIndexFile.toPath(), StandardOpenOption.WRITE)) {
        channel.truncate(truncateSize.right);
      }
    }
    this.keyFileSize = truncateSize.left;
    this.valueFileSize = truncateSize.right;
  }

  public void write(TsFileID tsFileID, Map<String, Long> tableSizeMap) throws IOException {
    if (keyFileOutputStream == null) {
      keyFileOutputStream = new FileOutputStream(currentKeyIndexFile, true);
      keyFileSize = currentKeyIndexFile.length();
      keyBufferedOutputStream = new BufferedOutputStream(keyFileOutputStream);
    }
    if (valueFileOutputStream == null) {
      valueFileOutputStream = new FileOutputStream(currentValueIndexFile, true);
      valueFileSize = currentValueIndexFile.length();
      valueBufferedOutputStream = new BufferedOutputStream(valueFileOutputStream);
    }

    long valueOffset = valueFileSize;
    valueFileSize +=
        ReadWriteForEncodingUtils.writeVarInt(tableSizeMap.size(), valueBufferedOutputStream);
    for (Map.Entry<String, Long> entry : tableSizeMap.entrySet()) {
      valueFileSize += ReadWriteIOUtils.writeVar(entry.getKey(), valueBufferedOutputStream);
      valueFileSize += ReadWriteIOUtils.write(entry.getValue(), valueBufferedOutputStream);
    }
    keyFileSize += ReadWriteIOUtils.write(KEY_FILE_RECORD_TYPE_OFFSET, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(tsFileID.timePartitionId, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(tsFileID.timestamp, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(tsFileID.fileVersion, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(tsFileID.compactionVersion, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(valueOffset, keyBufferedOutputStream);
  }

  public void write(TsFileID originTsFileID, TsFileID newTsFileID) throws IOException {
    if (keyFileOutputStream == null) {
      keyFileOutputStream = new FileOutputStream(currentKeyIndexFile, true);
      keyFileSize = currentKeyIndexFile.length();
      keyBufferedOutputStream = new BufferedOutputStream(keyFileOutputStream);
    }
    keyFileSize += ReadWriteIOUtils.write(KEY_FILE_RECORD_TYPE_REDIRECT, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(newTsFileID.timePartitionId, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(newTsFileID.timestamp, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(newTsFileID.fileVersion, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(newTsFileID.compactionVersion, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(originTsFileID.timestamp, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(originTsFileID.fileVersion, keyBufferedOutputStream);
    keyFileSize +=
        ReadWriteIOUtils.write(originTsFileID.compactionVersion, keyBufferedOutputStream);
  }

  public void flush() throws IOException {
    if (valueBufferedOutputStream != null) {
      valueBufferedOutputStream.flush();
    }
    if (keyFileOutputStream != null) {
      keyBufferedOutputStream.flush();
    }
  }

  public File getKeyFile() {
    return currentKeyIndexFile;
  }

  public File getValueFile() {
    return currentValueIndexFile;
  }

  public long keyFileLength() {
    return keyFileSize;
  }

  public long valueFileLength() {
    return valueFileSize;
  }

  public void sync() throws IOException {
    flush();
    if (valueFileOutputStream != null) {
      valueFileOutputStream.getFD().sync();
    }
    if (keyFileOutputStream != null) {
      keyFileOutputStream.getFD().sync();
    }
  }

  public void close() {
    try {
      sync();
    } catch (IOException ignored) {
    }
    try {
      if (valueBufferedOutputStream != null) {
        valueBufferedOutputStream.close();
        valueBufferedOutputStream = null;
        valueFileOutputStream = null;
      }
    } catch (IOException ignored) {
    }
    try {
      if (keyBufferedOutputStream != null) {
        keyBufferedOutputStream.close();
        keyBufferedOutputStream = null;
        keyFileOutputStream = null;
      }
    } catch (IOException ignored) {
    }
  }
}
