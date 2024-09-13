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

package org.apache.iotdb.db.storageengine.dataregion.utils.fileTimeIndexCache;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.getFileTimeIndexSerializedSize;

public class FileTimeIndexCacheReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileTimeIndexCacheReader.class);

  private final File logFile;
  private final long fileLength;
  private final int dataRegionId;

  public FileTimeIndexCacheReader(File logFile, String dataRegionId) {
    this.logFile = logFile;
    this.fileLength = logFile.length();
    this.dataRegionId = Integer.parseInt(dataRegionId);
  }

  public void read(Map<TsFileID, FileTimeIndex> fileTimeIndexMap) throws IOException {
    long readLength = 0L;
    try (DataInputStream logStream =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(logFile.toPath())))) {
      while (readLength < fileLength) {
        long partitionId = logStream.readLong();
        long timestamp = logStream.readLong();
        long fileVersion = logStream.readLong();
        long compactionVersion = logStream.readLong();
        long minStartTime = logStream.readLong();
        long maxEndTime = logStream.readLong();
        TsFileID tsFileID =
            new TsFileID(dataRegionId, partitionId, timestamp, fileVersion, compactionVersion);
        FileTimeIndex fileTimeIndex = new FileTimeIndex(minStartTime, maxEndTime);
        fileTimeIndexMap.put(tsFileID, fileTimeIndex);
        readLength += getFileTimeIndexSerializedSize();
      }
    } catch (IOException ignored) {
      // the error can be ignored
    }
    if (readLength != fileLength) {
      // if the file is complete, we can truncate the file
      try (FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.WRITE)) {
        channel.truncate(readLength);
      }
    }
  }
}
