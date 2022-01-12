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

package org.apache.iotdb.db.engine.compaction.cross.inplace.recover;

import org.apache.iotdb.db.engine.compaction.TsFileIdentifier;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/** MergeLogger records the progress of a merge in file "merge.log" as text lines. */
public class InplaceCompactionLogger {

  public static final String MERGE_LOG_NAME = "merge.log";

  public static final String STR_SEQ_FILES = "seqFiles";
  public static final String STR_TARGET_FILES = "targetFiles";
  public static final String STR_UNSEQ_FILES = "unseqFiles";
  public static final String MAGIC_STRING = "crossSpaceCompaction";
  static final String STR_TIMESERIES = "timeseries";
  static final String STR_START = "start";
  static final String STR_END = "end";
  static final String STR_ALL_TS_END = "all ts end";
  static final String STR_MERGE_START = "merge start";
  static final String STR_MERGE_END = "merge end";

  private BufferedWriter logStream;

  public InplaceCompactionLogger(String storageGroupDir) throws IOException {
    logStream = new BufferedWriter(new FileWriter(new File(storageGroupDir, MERGE_LOG_NAME), true));
    logStringInfo(MAGIC_STRING);
  }

  public void close() throws IOException {
    logStream.close();
  }

  public void logStringInfo(String logInfo) throws IOException {
    logStream.write(logInfo);
    logStream.newLine();
    logStream.flush();
  }

  public void logTSStart(List<PartialPath> paths) throws IOException {
    logStream.write(STR_START);
    for (PartialPath path : paths) {
      logStream.write(" " + path);
    }
    logStream.newLine();
    logStream.flush();
  }

  public void logFilePosition(File file) throws IOException {
    logStream.write(String.format("%s %d", file.getAbsolutePath(), file.length()));
    logStream.newLine();
    logStream.flush();
  }

  public void logTSEnd() throws IOException {
    logStringInfo(STR_END);
  }

  public void logAllTsEnd() throws IOException {
    logStringInfo(STR_ALL_TS_END);
  }

  public void logFiles(CrossSpaceMergeResource resource) throws IOException {
    logFiles(resource.getSeqFiles(), STR_SEQ_FILES);
    logFiles(resource.getUnseqFiles(), STR_UNSEQ_FILES);
  }

  public void logFiles(List<TsFileResource> seqFiles, String flag) throws IOException {
    logStream.write(flag);
    logStream.newLine();
    for (TsFileResource tsFileResource : seqFiles) {
      logStream.write(
          TsFileIdentifier.getFileIdentifierFromFilePath(
                  tsFileResource.getTsFile().getAbsolutePath())
              .toString());
      logStream.newLine();
    }
    logStream.flush();
  }

  public static File[] findCrossSpaceCompactionLogs(String directory) {
    File timePartitionDir = new File(directory);
    if (timePartitionDir.exists()) {
      return timePartitionDir.listFiles((dir, name) -> name.endsWith(MERGE_LOG_NAME));
    } else {
      return new File[0];
    }
  }
}
