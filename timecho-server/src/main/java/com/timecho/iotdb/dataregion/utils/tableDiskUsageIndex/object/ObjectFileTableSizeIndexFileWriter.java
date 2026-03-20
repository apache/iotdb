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

package com.timecho.iotdb.dataregion.utils.tableDiskUsageIndex.object;

import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.DataRegionTableSizeQueryContext;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.TimePartitionTableSizeQueryContext;

import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class ObjectFileTableSizeIndexFileWriter {
  private final int regionId;
  private final File file;
  private long snapshotSize;
  private BufferedOutputStream bufferedOutputStream;
  private FileOutputStream fileOutputStream;

  public ObjectFileTableSizeIndexFileWriter(int regionId, File file, boolean needSelfCheck)
      throws IOException {
    this.regionId = regionId;
    this.file = file;
    file.createNewFile();
    if (needSelfCheck) {
      selfCheckAndRecover();
    } else {
      snapshotSize = file.length();
    }
  }

  private void selfCheckAndRecover() throws IOException {
    long truncateSize = 0;
    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(true);
    ObjectTableSizeIndexReader reader = new ObjectTableSizeIndexReader(file, file.length());
    try {
      truncateSize = reader.selfCheck(context);
      this.snapshotSize = reader.getSnapshotSize();
    } finally {
      reader.close();
    }
    if (truncateSize != file.length()) {
      try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
        fileChannel.truncate(truncateSize);
      }
    }
    FileMetrics.getInstance().increaseObjectFileNum(context.getObjectFileNum());
    FileMetrics.getInstance().increaseObjectFileSize(context.getObjectFileSize());
  }

  public void writeResultFromQuery(DataRegionTableSizeQueryContext queryContext)
      throws IOException {
    for (Map.Entry<Long, TimePartitionTableSizeQueryContext> entry :
        queryContext.getTimePartitionTableSizeQueryContextMap().entrySet()) {
      long timePartition = entry.getKey();
      Map<String, Long> timePartitionTableSizeMap = entry.getValue().getTableSizeResultMap();
      write(timePartition, timePartitionTableSizeMap);
    }
    write(queryContext.getObjectFileNum());
    writeSnapshotEndMark();
  }

  public void write(int numDelta) throws IOException {
    ensureOpen();
    ReadWriteIOUtils.write(
        ObjectTableSizeIndexWriter.OBJECT_FILE_NUM_DELTA_TYPE, bufferedOutputStream);
    ReadWriteForEncodingUtils.writeVarInt(numDelta, bufferedOutputStream);
  }

  public void write(long timePartition, Map<String, Long> deltaMap) throws IOException {
    ensureOpen();
    ReadWriteIOUtils.write(
        ObjectTableSizeIndexWriter.OBJECT_FILE_SIZE_DELTA_TYPE, bufferedOutputStream);
    ReadWriteIOUtils.write(timePartition, bufferedOutputStream);
    ReadWriteForEncodingUtils.writeVarInt(deltaMap.size(), bufferedOutputStream);
    for (Map.Entry<String, Long> entry : deltaMap.entrySet()) {
      ReadWriteIOUtils.writeVar(entry.getKey(), bufferedOutputStream);
      ReadWriteIOUtils.write(entry.getValue(), bufferedOutputStream);
    }
  }

  public void writeSnapshotEndMark() throws IOException {
    ensureOpen();
    ReadWriteIOUtils.write(
        ObjectTableSizeIndexWriter.OBJECT_FILE_SNAPSHOT_END_TYPE, bufferedOutputStream);
  }

  public File getFile() {
    return file;
  }

  public long length() {
    return file.length();
  }

  public long getSnapshotSize() {
    return snapshotSize;
  }

  private void ensureOpen() throws IOException {
    if (fileOutputStream == null) {
      fileOutputStream = new FileOutputStream(file, true);
      bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
    }
  }

  public void flush() throws IOException {
    if (fileOutputStream == null) {
      return;
    }
    bufferedOutputStream.flush();
  }

  public void sync() throws IOException {
    if (fileOutputStream == null) {
      return;
    }
    bufferedOutputStream.flush();
    fileOutputStream.getFD().sync();
  }

  public void close() {
    if (fileOutputStream == null) {
      return;
    }
    try {
      bufferedOutputStream.close();
      bufferedOutputStream = null;
      fileOutputStream = null;
    } catch (IOException ignored) {
    }
  }
}
