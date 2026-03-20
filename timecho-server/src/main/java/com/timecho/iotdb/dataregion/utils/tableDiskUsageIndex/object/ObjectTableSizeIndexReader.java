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

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.DataRegionTableSizeQueryContext;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.object.IObjectTableSizeIndexReader;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.tsfile.TsFileTableSizeIndexReader;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.io.IOException;

public class ObjectTableSizeIndexReader implements IObjectTableSizeIndexReader {

  private final File file;
  private final long readableLength;
  private TsFileTableSizeIndexReader.DirectBufferedSeekableFileInputStream inputStream;

  private long snapshotSize;

  public ObjectTableSizeIndexReader(File file, long fileLength) {
    this.file = file;
    this.readableLength = fileLength;
  }

  public long selfCheck(DataRegionTableSizeQueryContext context) {
    if (!file.exists()) {
      return 0;
    }
    long truncateSize = 0;
    try {
      ensureOpen();
      while (inputStream.position() < readableLength) {
        readOneEntry(context);
        truncateSize = inputStream.position();
      }
    } catch (Exception ignored) {
      // This is expected because the index file may not complete
    } finally {
      close();
    }
    return truncateSize;
  }

  public long getSnapshotSize() {
    return snapshotSize;
  }

  @Override
  public boolean loadObjectFileTableSize(
      DataRegionTableSizeQueryContext dataRegionContext, long startTime, long maxRunTime)
      throws IOException {
    if (readableLength <= 0) {
      return true;
    }
    ensureOpen();
    do {
      readOneEntry(dataRegionContext);
      if (inputStream.position() >= readableLength) {
        close();
        return true;
      }
    } while (System.currentTimeMillis() - startTime < maxRunTime);
    return false;
  }

  private void ensureOpen() throws IOException {
    if (inputStream == null) {
      inputStream =
          new TsFileTableSizeIndexReader.DirectBufferedSeekableFileInputStream(
              file.toPath(), 4 * 1024);
    }
  }

  private void readOneEntry(DataRegionTableSizeQueryContext dataRegionContext) throws IOException {
    byte type = ReadWriteIOUtils.readByte(inputStream);
    switch (type) {
      case ObjectTableSizeIndexWriter.OBJECT_FILE_NUM_DELTA_TYPE:
        int numDelta = ReadWriteForEncodingUtils.readVarInt(inputStream);
        dataRegionContext.updateObjectFileNum(numDelta);
        break;
      case ObjectTableSizeIndexWriter.OBJECT_FILE_SIZE_DELTA_TYPE:
        long timePartition = ReadWriteIOUtils.readLong(inputStream);
        int tableNum = ReadWriteForEncodingUtils.readVarInt(inputStream);
        for (int i = 0; i < tableNum; i++) {
          String table = ReadWriteIOUtils.readVarIntString(inputStream);
          long sizeDelta = ReadWriteIOUtils.readLong(inputStream);
          dataRegionContext.updateResult(table, sizeDelta, timePartition);
        }
        break;
      case ObjectTableSizeIndexWriter.OBJECT_FILE_SNAPSHOT_END_TYPE:
        snapshotSize = inputStream.position();
        break;
      default:
        throw new IoTDBRuntimeException(
            "Unsupported record type in file: " + file.getPath() + ", type: " + type,
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  @Override
  public void close() {
    if (inputStream != null) {
      try {
        inputStream.close();
      } catch (IOException ignored) {
      }
    }
  }
}
