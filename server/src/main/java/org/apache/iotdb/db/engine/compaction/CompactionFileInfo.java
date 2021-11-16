/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import java.io.File;

/**
 * This class record the logical information of files in compaction, which is used to locate file in
 * disk. File information includes is file sequence, storage group name, virtual storage group id,
 * time partition id, file name. <b>This class cannot be initialized directly.</b> We provide some
 * static method to create instance of this class.
 */
public class CompactionFileInfo {
  private final String logicalStorageGroupName;
  private final String virtualStorageGroupId;
  private final String timePartitionId;
  private final boolean sequence;
  private final String filename;
  public static final String INFO_SEPARATOR = " ";

  private CompactionFileInfo(
      String logicalStorageGroupName,
      String virtualStorageGroupId,
      String timePartitionId,
      boolean sequence,
      String filename) {
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupId = virtualStorageGroupId;
    this.timePartitionId = timePartitionId;
    this.sequence = sequence;
    this.filename = filename;
  }

  /**
   * This function generates an instance of CompactionFileInfo by parsing the path of a tsfile.
   * Notice, the path of the file should include information of its logical storage group, virtual
   * storage group id, time partition, sequence or not and its filename, such as
   * "sequence/root.test.sg/0/0/1-1-0-0.tsfile".
   */
  public static CompactionFileInfo getFileInfoFromFilePath(String filepath) {
    String splitter = File.separator;
    if (splitter.equals("\\")) {
      // String.split use a regex way to split the string into array
      // "\" Should be escaped as "\\"
      splitter = "\\\\";
    }
    String[] splittedPath = filepath.split(splitter);
    int length = splittedPath.length;
    if (length < 5) {
      // if the path contains the required information
      // after splitting result should contain more than 5 objects
      throw new RuntimeException(
          String.format("Path %s cannot be parsed into file info", filepath));
    }

    return new CompactionFileInfo(
        splittedPath[length - 4],
        splittedPath[length - 3],
        splittedPath[length - 2],
        splittedPath[length - 5].equals("sequence"),
        splittedPath[length - 1]);
  }

  /**
   * This function generates an instance of CompactionFileInfo by parsing the info string of a
   * tsfile(usually recorded in a compaction.log), such as “root.test.sg 0 0 true 1-1-0-0.tsfile"
   */
  public static CompactionFileInfo getFileInfoFromInfoString(String infoString) {
    String[] splittedFileInfo = infoString.split(INFO_SEPARATOR);
    int length = splittedFileInfo.length;
    if (length != 5) {
      throw new RuntimeException(
          String.format("String %s is not a legal file info string", infoString));
    }
    return new CompactionFileInfo(
        splittedFileInfo[0],
        splittedFileInfo[1],
        splittedFileInfo[2],
        Boolean.parseBoolean(splittedFileInfo[3]),
        splittedFileInfo[4]);
  }

  @Override
  public String toString() {
    return String.format(
        "%s%s%s%s%s%s%s%s%s",
        logicalStorageGroupName,
        INFO_SEPARATOR,
        virtualStorageGroupId,
        INFO_SEPARATOR,
        timePartitionId,
        INFO_SEPARATOR,
        sequence,
        INFO_SEPARATOR,
        filename);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CompactionFileInfo)) {
      return false;
    }
    CompactionFileInfo otherInfo = (CompactionFileInfo) other;
    return otherInfo.sequence == this.sequence
        && otherInfo.logicalStorageGroupName.equals(this.logicalStorageGroupName)
        && otherInfo.virtualStorageGroupId.equals(this.virtualStorageGroupId)
        && otherInfo.timePartitionId.equals(this.timePartitionId)
        && otherInfo.filename.equals(this.filename);
  }

  /**
   * This method find the File object of current file by searching it in every data directory. If
   * the file is not found, it will return null.
   */
  public File getFileFromDataDirs() {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    String partialFileString =
        (sequence ? "sequence" : "unsequence")
            + File.separator
            + logicalStorageGroupName
            + File.separator
            + virtualStorageGroupId
            + File.separator
            + timePartitionId
            + File.separator
            + filename;
    for (String dataDir : dataDirs) {
      File file = new File(dataDir, partialFileString);
      if (file.exists()) {
        return file;
      }
    }
    return null;
  }

  public String getFilename() {
    return filename;
  }

  public String getLogicalStorageGroupName() {
    return logicalStorageGroupName;
  }

  public String getVirtualStorageGroupId() {
    return virtualStorageGroupId;
  }

  public String getTimePartitionId() {
    return timePartitionId;
  }

  public boolean isSequence() {
    return sequence;
  }
}
