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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import java.io.File;

/**
 * This class record the logical information of files in compaction, which is used to locate file in
 * disk. File identifier includes whether the file is sequence, its storage group name, virtual
 * storage group id, time partition id and file name. <b>This class cannot be initialized
 * directly.</b> We provide some static methods to create instance of this class.
 */
public class TsFileIdentifier {
  private final String logicalStorageGroupName;
  private final String virtualStorageGroupId;
  private final String timePartitionId;
  private final boolean sequence;
  private final String filename;
  public static final String INFO_SEPARATOR = " ";
  public static final int FILE_NAME_OFFSET_IN_PATH = 1;
  public static final int TIME_PARTITION_OFFSET_IN_PATH = 2;
  public static final int VIRTUAL_SG_OFFSET_IN_PATH = 3;
  public static final int LOGICAL_SG_OFFSET_IN_PATH = 4;
  public static final int SEQUENCE_OFFSET_IN_PATH = 5;
  public static final int LOGICAL_SG_OFFSET_IN_LOG = 0;
  public static final int VIRTUAL_SG_OFFSET_IN_LOG = 1;
  public static final int TIME_PARTITION_OFFSET_IN_LOG = 2;
  public static final int SEQUENCE_OFFSET_IN_LOG = 3;
  public static final int FILE_NAME_OFFSET_IN_LOG = 4;

  private TsFileIdentifier(
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
   * This function generates an instance of CompactionFileIdentifier by parsing the path of a
   * tsfile. Notice, the path of the file should include information of its logical storage group,
   * virtual storage group id, time partition, sequence or not and its filename, such as
   * "sequence/root.test.sg/0/0/1-1-0-0.tsfile".
   */
  public static TsFileIdentifier getFileIdentifierFromFilePath(String filepath) {
    String splitter = File.separator;
    if ("\\".equals(splitter)) {
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

    return new TsFileIdentifier(
        splittedPath[length - LOGICAL_SG_OFFSET_IN_PATH],
        splittedPath[length - VIRTUAL_SG_OFFSET_IN_PATH],
        splittedPath[length - TIME_PARTITION_OFFSET_IN_PATH],
        splittedPath[length - SEQUENCE_OFFSET_IN_PATH].equals(IoTDBConstant.SEQUENCE_FLODER_NAME),
        splittedPath[length - FILE_NAME_OFFSET_IN_PATH]);
  }

  /**
   * This function generates an instance of CompactionFileIdentifier by parsing the info string of a
   * tsfile(usually recorded in a compaction.log), such as “root.test.sg 0 0 true 1-1-0-0.tsfile"
   */
  public static TsFileIdentifier getFileIdentifierFromInfoString(String infoString) {
    String[] splittedFileInfo = infoString.split(INFO_SEPARATOR);
    int length = splittedFileInfo.length;
    if (length != 5) {
      throw new RuntimeException(
          String.format("String %s is not a legal file info string", infoString));
    }
    return new TsFileIdentifier(
        splittedFileInfo[LOGICAL_SG_OFFSET_IN_LOG],
        splittedFileInfo[VIRTUAL_SG_OFFSET_IN_LOG],
        splittedFileInfo[TIME_PARTITION_OFFSET_IN_LOG],
        Boolean.parseBoolean(splittedFileInfo[SEQUENCE_OFFSET_IN_LOG]),
        splittedFileInfo[FILE_NAME_OFFSET_IN_LOG]);
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
    if (!(other instanceof TsFileIdentifier)) {
      return false;
    }
    TsFileIdentifier otherInfo = (TsFileIdentifier) other;
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
        (sequence ? IoTDBConstant.SEQUENCE_FLODER_NAME : IoTDBConstant.UNSEQUENCE_FLODER_NAME)
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

  public String getFilePath() {
    return (sequence ? IoTDBConstant.SEQUENCE_FLODER_NAME : IoTDBConstant.UNSEQUENCE_FLODER_NAME)
        + File.separator
        + logicalStorageGroupName
        + File.separator
        + virtualStorageGroupId
        + File.separator
        + timePartitionId
        + File.separator
        + filename;
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
