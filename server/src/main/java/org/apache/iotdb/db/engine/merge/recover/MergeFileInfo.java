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
package org.apache.iotdb.db.engine.merge.recover;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.compaction.InvalidCompactionLogException;

import java.io.File;

public class MergeFileInfo {
  String logicalStorageGroup;
  String virtualStorageGroup;
  long timePartition;
  String filename;
  boolean sequence;

  private MergeFileInfo(
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartition,
      String filename,
      boolean sequence) {
    this.logicalStorageGroup = logicalStorageGroup;
    this.virtualStorageGroup = virtualStorageGroup;
    this.timePartition = timePartition;
    this.filename = filename;
    this.sequence = sequence;
  }

  public static MergeFileInfo getFileInfoFromFile(File file) {
    String filePath = file.getAbsolutePath();
    String splitSeparator = File.separator;
    if (splitSeparator.equals("\\")) {
      // in regex, split word should be \\
      splitSeparator = "\\\\";
    }

    String[] paths = filePath.split(splitSeparator);
    int pathLength = paths.length;
    return new MergeFileInfo(
        paths[pathLength - 4],
        paths[pathLength - 3],
        Long.parseLong(paths[pathLength - 2]),
        paths[pathLength - 1],
        paths[pathLength - 5].equals("sequence"));
  }

  public static MergeFileInfo getFileInfoFromString(String infoString)
      throws InvalidCompactionLogException {
    if (!infoString.contains(File.separator)) {
      // the info string records info of merge files
      String[] splits = infoString.split(" ");
      if (splits.length != 5) {
        throw new InvalidCompactionLogException("Invalid file info string " + infoString);
      }

      return new MergeFileInfo(
          splits[0],
          splits[1],
          Long.parseLong(splits[2]),
          splits[3],
          Boolean.parseBoolean(splits[4]));
    } else {
      // the info string records path of merge files
      return getFileInfoFromFile(new File(infoString));
    }
  }

  public File getFileFromDataDirs() {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    for (String dataDir : dataDirs) {
      File file =
          new File(
              dataDir.concat(File.separator)
                  + (sequence ? "sequence" : "unsequence").concat(File.separator)
                  + logicalStorageGroup.concat(File.separator)
                  + virtualStorageGroup.concat(File.separator)
                  + String.valueOf(timePartition).concat(File.separator)
                  + filename);
      if (file.exists()) {
        return file;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return String.format(
        "%s %s %d %s %s",
        logicalStorageGroup, virtualStorageGroup, timePartition, filename, sequence);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof MergeFileInfo) {
      MergeFileInfo otherInfo = (MergeFileInfo) other;
      return logicalStorageGroup.equals(otherInfo.logicalStorageGroup)
          && virtualStorageGroup.equals(otherInfo.virtualStorageGroup)
          && timePartition == otherInfo.timePartition
          && sequence == otherInfo.sequence
          && filename.equals(otherInfo.filename);
    }
    return false;
  }
}
