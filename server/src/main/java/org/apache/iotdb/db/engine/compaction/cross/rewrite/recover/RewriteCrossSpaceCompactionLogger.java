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

package org.apache.iotdb.db.engine.compaction.cross.rewrite.recover;

import org.apache.iotdb.db.engine.compaction.TsFileIdentifier;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.manage.CrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/** MergeLogger records the progress of a merge in file "merge.log" as text lines. */
public class RewriteCrossSpaceCompactionLogger implements AutoCloseable {

  public static final String COMPACTION_LOG_NAME = "cross-compaction.log";
  public static final String COMPACTION_LOG_NAME_FEOM_OLD = "merge.log";

  public static final String STR_SEQ_FILES = "seqFiles";
  public static final String STR_TARGET_FILES = "targetFiles";
  public static final String STR_UNSEQ_FILES = "unseqFiles";
  public static final String MAGIC_STRING = "crossSpaceCompaction";

  private BufferedWriter logStream;

  public RewriteCrossSpaceCompactionLogger(File logFile) throws IOException {
    logStream = new BufferedWriter(new FileWriter(logFile, true));
    logStringInfo(MAGIC_STRING);
  }

  @Override
  public void close() throws IOException {
    logStream.close();
  }

  public void logStringInfo(String logInfo) throws IOException {
    logStream.write(logInfo);
    logStream.newLine();
    logStream.flush();
  }

  public void logFiles(CrossSpaceCompactionResource resource) throws IOException {
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
      return timePartitionDir.listFiles((dir, name) -> name.endsWith(COMPACTION_LOG_NAME));
    } else {
      return new File[0];
    }
  }
}
