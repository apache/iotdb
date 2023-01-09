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

package org.apache.iotdb.db.engine.compaction.execute.utils.log;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/** MergeLogger records the progress of a merge in file "merge.log" as text lines. */
public class CompactionLogger implements AutoCloseable {

  public static final String CROSS_COMPACTION_LOG_NAME_SUFFIX = ".cross-compaction.log";
  public static final String CROSS_COMPACTION_LOG_NAME_FROM_OLD = "merge.log";
  public static final String INNER_COMPACTION_LOG_NAME_SUFFIX = ".inner-compaction.log";
  public static final String INNER_COMPACTION_LOG_NAME_SUFFIX_FROM_OLD = ".compaction.log";

  public static final String STR_SOURCE_FILES = "source";
  public static final String STR_TARGET_FILES = "target";

  public static final String STR_DELETED_TARGET_FILES = "empty";

  public static final String STR_SOURCE_FILES_FROM_OLD = "info-source";
  public static final String STR_TARGET_FILES_FROM_OLD = "info-target";
  public static final String STR_SEQ_FILES_FROM_OLD = "seqFiles";
  public static final String STR_UNSEQ_FILES_FROM_OLD = "unseqFiles";
  public static final String SEQUENCE_NAME_FROM_OLD = "sequence";
  public static final String UNSEQUENCE_NAME_FROM_OLD = "unsequence";
  public static final String STR_MERGE_START_FROM_OLD = "merge start";

  private BufferedWriter logStream;

  public CompactionLogger(File logFile) throws IOException {
    logStream = new BufferedWriter(new FileWriter(logFile, true));
  }

  @Override
  public void close() throws IOException {
    logStream.close();
  }

  public void logFiles(List<TsFileResource> tsFiles, String flag) throws IOException {
    for (TsFileResource tsFileResource : tsFiles) {
      logStream.write(
          flag
              + TsFileIdentifier.INFO_SEPARATOR
              + TsFileIdentifier.getFileIdentifierFromFilePath(
                      tsFileResource.getTsFile().getAbsolutePath())
                  .toString());
      logStream.newLine();
    }
    logStream.flush();
  }

  public void logFile(TsFileResource tsFile, String flag) throws IOException {
    logStream.write(
        flag
            + TsFileIdentifier.INFO_SEPARATOR
            + TsFileIdentifier.getFileIdentifierFromFilePath(tsFile.getTsFile().getAbsolutePath())
                .toString());
    logStream.newLine();
    logStream.flush();
  }

  public static File[] findCompactionLogs(boolean isInnerSpace, String directory) {
    String compactionLogSuffix =
        isInnerSpace ? INNER_COMPACTION_LOG_NAME_SUFFIX : CROSS_COMPACTION_LOG_NAME_SUFFIX;
    File timePartitionDir = new File(directory);
    if (timePartitionDir.exists()) {
      return timePartitionDir.listFiles((dir, name) -> name.endsWith(compactionLogSuffix));
    } else {
      return new File[0];
    }
  }
}
