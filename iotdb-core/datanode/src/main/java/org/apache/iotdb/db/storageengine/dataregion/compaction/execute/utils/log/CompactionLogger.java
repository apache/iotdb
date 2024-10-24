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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log;

import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/** MergeLogger records the progress of a merge in file "merge.log" as text lines. */
public class CompactionLogger implements AutoCloseable {

  public static final String CROSS_COMPACTION_LOG_NAME_SUFFIX = ".cross-compaction.log";
  public static final String INNER_COMPACTION_LOG_NAME_SUFFIX = ".inner-compaction.log";
  public static final String INSERTION_COMPACTION_LOG_NAME_SUFFIX = ".insertion-compaction.log";
  public static final String SETTLE_COMPACTION_LOG_NAME_SUFFIX = ".settle-compaction.log";

  public static final String STR_SOURCE_FILES = "source";
  public static final String STR_TARGET_FILES = "target";
  public static final String STR_DELETED_TARGET_FILES = "empty";
  private FileOutputStream logStream;

  public CompactionLogger(File logFile) throws IOException {
    if (!logFile.getParentFile().exists()) {
      logFile.getParentFile().mkdirs();
    }
    logStream = new FileOutputStream(logFile, true);
  }

  @Override
  public void close() throws IOException {
    logStream.close();
  }

  public void logFiles(List<TsFileResource> tsFiles, String flag) throws IOException {
    for (TsFileResource tsFileResource : tsFiles) {
      logFile(tsFileResource, flag);
    }
  }

  public void logTaskStage(CompactionTaskStage stage) throws IOException {
    logStream.write(stage.name().getBytes());
    logStream.write(System.lineSeparator().getBytes());
    logStream.flush();
  }

  public void logFile(TsFileResource tsFile, String flag) throws IOException {
    String log =
        flag
            + TsFileIdentifier.INFO_SEPARATOR
            + TsFileIdentifier.getFileIdentifierFromFilePath(tsFile.getTsFile().getAbsolutePath());
    logStream.write(log.getBytes());
    logStream.write(System.lineSeparator().getBytes());
    logStream.flush();
  }

  public static File[] findCompactionLogs(boolean isInnerSpace, String directory) {
    File timePartitionDir = new File(directory);
    if (timePartitionDir.exists()) {
      return timePartitionDir.listFiles(
          (dir, name) -> {
            if (isInnerSpace) {
              return name.endsWith(INNER_COMPACTION_LOG_NAME_SUFFIX);
            } else {
              return name.endsWith(CROSS_COMPACTION_LOG_NAME_SUFFIX)
                  || name.endsWith(INSERTION_COMPACTION_LOG_NAME_SUFFIX);
            }
          });
    } else {
      return new File[0];
    }
  }

  public static File[] findCompactionLogs(CompactionTaskType type, File timePartitionDir) {
    if (timePartitionDir.exists()) {
      String logNameSuffix = getLogSuffix(type);
      return timePartitionDir.listFiles((dir, name) -> name.endsWith(logNameSuffix));
    } else {
      return new File[0];
    }
  }

  public static String getLogSuffix(CompactionTaskType type) {
    String logNameSuffix = null;
    switch (type) {
      case INNER_SEQ:
      case INNER_UNSEQ:
      case REPAIR:
        logNameSuffix = INNER_COMPACTION_LOG_NAME_SUFFIX;
        break;
      case CROSS:
        logNameSuffix = CROSS_COMPACTION_LOG_NAME_SUFFIX;
        break;
      case INSERTION:
        logNameSuffix = INSERTION_COMPACTION_LOG_NAME_SUFFIX;
        break;
      case SETTLE:
        logNameSuffix = SETTLE_COMPACTION_LOG_NAME_SUFFIX;
        break;
      default:
        break;
    }
    return logNameSuffix;
  }

  /**
   * call system fsync function, make sure data flush to disk
   *
   * @throws IOException
   */
  public void force() throws IOException {
    logStream.flush();
    logStream.getFD().sync();
  }
}
