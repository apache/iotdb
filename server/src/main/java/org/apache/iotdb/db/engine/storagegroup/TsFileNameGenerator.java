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

package org.apache.iotdb.db.engine.storagegroup;

import java.io.IOException;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import java.io.File;

public class TsFileNameGenerator {

  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  /**
   * @param sequence whether the file is sequence
   * @param logicalStorageGroup eg. "root.sg"
   * @param virtualStorageGroup eg. "0"
   * @param timePartitionId eg. 0
   * @param time eg. 1623895965058
   * @param version eg. 0
   * @param innerSpaceCompactionCount the times of inner space compaction of this file
   * @param crossSpaceCompactionCount the times of cross space compaction of this file
   * @return a relative path of new tsfile, eg.
   *     "data/data/sequence/root.sg/0/0/1623895965058-0-0-0.tsfile"
   */
  public static String generateNewTsFilePath(
      boolean sequence,
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartitionId,
      long time,
      long version,
      int innerSpaceCompactionCount,
      int crossSpaceCompactionCount)
      throws DiskSpaceInsufficientException {
    String tsFileDir = generateTsFileDir(sequence, logicalStorageGroup, virtualStorageGroup,
        timePartitionId);
    return tsFileDir + File.separator + generateNewTsFileName(time, version,
        innerSpaceCompactionCount, crossSpaceCompactionCount);
  }

  public static String generateNewTsFilePath(
      String tsFileDir,
      long time,
      long version,
      int innerSpaceCompactionCount,
      int crossSpaceCompactionCount) {
    return tsFileDir + File.separator + generateNewTsFileName(time, version,
        innerSpaceCompactionCount, crossSpaceCompactionCount);
  }

  public static String generateNewTsFilePatWithMkdir(
      boolean sequence,
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartitionId,
      long time,
      long version,
      int innerSpaceCompactionCount,
      int crossSpaceCompactionCount)
      throws DiskSpaceInsufficientException, IOException {
    String tsFileDir = generateTsFileDir(sequence, logicalStorageGroup, virtualStorageGroup,
        timePartitionId);
    boolean result = fsFactory.getFile(tsFileDir).mkdirs();
    if (!result) {
      throw new IOException(String.format("mkdirs %s failed!", tsFileDir));
    }
    return tsFileDir + File.separator + generateNewTsFileName(time, version,
        innerSpaceCompactionCount, crossSpaceCompactionCount);
  }

  private static String generateTsFileDir(
      boolean sequence,
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartitionId)
      throws DiskSpaceInsufficientException {
    DirectoryManager directoryManager = DirectoryManager.getInstance();
    String baseDir =
        sequence
            ? directoryManager.getNextFolderForSequenceFile()
            : directoryManager.getNextFolderForUnSequenceFile();
    return baseDir + File.separator + logicalStorageGroup + File.separator + virtualStorageGroup
        + File.separator + timePartitionId;
  }

  public static String generateNewTsFileName(
      long time, long version, int innerSpaceCompactionCount, int crossSpaceCompactionCount) {
    return time
        + IoTDBConstant.FILE_NAME_SEPARATOR
        + version
        + IoTDBConstant.FILE_NAME_SEPARATOR
        + innerSpaceCompactionCount
        + IoTDBConstant.FILE_NAME_SEPARATOR
        + crossSpaceCompactionCount
        + TsFileConstant.TSFILE_SUFFIX;
  }
}
