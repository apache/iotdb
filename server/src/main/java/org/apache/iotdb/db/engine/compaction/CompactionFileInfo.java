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
    String[] splittedPath;
    return null;
  }
}
