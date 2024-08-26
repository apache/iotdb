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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.getFileTimeIndexSerializedSize;

public class FileTimeIndexCacheReader {

  private final File logFile;
  private final long fileLength;
  private final int dataRegionId;
  private final long partitionId;

  public FileTimeIndexCacheReader(File logFile, String dataRegionId, long partitionId) {
    this.logFile = logFile;
    this.fileLength = logFile.length();
    this.dataRegionId = Integer.parseInt(dataRegionId);
    this.partitionId = partitionId;
  }

  public Map<TsFileID, FileTimeIndex> read() throws IOException {
    Map<TsFileID, FileTimeIndex> fileTimeIndexMap = new HashMap<>();
    try (DataInputStream logStream =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(logFile.toPath())))) {
      long readLength = 0L;
      while (readLength < fileLength) {
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
    }
    return fileTimeIndexMap;
  }
}
