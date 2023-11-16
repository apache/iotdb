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

package org.apache.iotdb.db.storageengine.dataregion.tsfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.tsfile.utils.FilePathUtils.splitTsFilePath;

public class TsFileID {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileID.class);

  public final int regionId;
  public final long timePartitionId;
  public final long fileVersion;
  // high 32 bit is compaction level, low 32 bit is merge count
  public final long compactionVersion;

  public TsFileID() {
    this.regionId = -1;
    this.timePartitionId = -1;
    this.fileVersion = -1;
    this.compactionVersion = -1;
  }

  public TsFileID(int regionId, long timePartitionId, long fileVersion, long compactionVersion) {
    this.regionId = regionId;
    this.timePartitionId = timePartitionId;
    this.fileVersion = fileVersion;
    this.compactionVersion = compactionVersion;
  }

  public TsFileID(String tsFileAbsolutePath) {
    int tmpRegionId = -1;
    long tmpTimePartitionId = -1;
    String[] pathSegments = splitTsFilePath(tsFileAbsolutePath);
    int pathLength = pathSegments.length;
    if (pathLength >= 3) {
      try {
        tmpRegionId = Integer.parseInt(pathSegments[pathLength - 3]);
      } catch (NumberFormatException e) {
        // ignore, load will get in here
      }
      try {
        tmpTimePartitionId = Long.parseLong(pathSegments[pathLength - 2]);
      } catch (NumberFormatException e) {
        // ignore, load will get in here
      }
    }

    this.regionId = tmpRegionId;
    this.timePartitionId = tmpTimePartitionId;
    long[] arr = splitAndGetVersionArray(pathSegments[pathLength - 1]);
    this.fileVersion = arr[0];
    this.compactionVersion = arr[1];
  }

  /**
   * @return a long array whose length is 2, the first long value is tsfile version, second long
   *     value is compaction version, high 32 bit is in-space compaction count, low 32 bit is
   *     cross-space compaction count
   */
  private static long[] splitAndGetVersionArray(String tsFileName) {
    String[] names = tsFileName.split(FILE_NAME_SEPARATOR);
    long[] versionArray = new long[2];
    if (names.length != 4) {
      // ignore,  load will get in here
      return versionArray;
    }
    versionArray[0] = Long.parseLong(names[1]);

    int dotIndex = names[3].indexOf(".");
    versionArray[1] =
        (Long.parseLong(names[2]) << 32) | Long.parseLong(names[3].substring(0, dotIndex));
    return versionArray;
  }
}
