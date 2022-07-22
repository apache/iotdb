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

package org.apache.iotdb.db.engine.alter.log;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.compaction.log.TsFileIdentifier;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Alteringlogger records the progress of modifying the encoding compression method in the form of
 * text lines in the file "alter.log".
 */
public class AlertingLogger implements AutoCloseable {

  public static final String ALTERING_LOG_NAME = "alter.log";
  public static final String FLAG_TIME_PARTITIONS_HEADER_DONE = "ftphd";
  public static final String FLAG_TIME_PARTITION_START = "ftps";
  public static final String FLAG_TIME_PARTITION_DONE = "ftpd";
  public static final String FLAG_SEQ = "1";
  public static final String FLAG_UNSEQ = "0";
  public static final String FLAG_INIT_SELECTED_FILE = "fisf";
  public static final String FLAG_TARGET_FILE = "fitf";
  public static final String FLAG_DONE = "done";

  private BufferedWriter logStream;

  public AlertingLogger(File logFile) throws IOException {
    logStream = new BufferedWriter(new FileWriter(logFile, true));
  }

  @Override
  public void close() throws IOException {
    logStream.close();
  }

  public void logHeader(
      PartialPath fullPath,
      TSEncoding curEncoding,
      CompressionType curCompressionType,
      Set<Long> timePartitions)
      throws IOException {

    if (fullPath == null || curEncoding == null || curCompressionType == null) {
      throw new IOException("alter params is null");
    }
    logStream.write(fullPath.getFullPath());
    logStream.write(curEncoding.serialize());
    logStream.write(curCompressionType.serialize());
    logStream.newLine();
    for (long timePartition : timePartitions) {
      logStream.write(Long.toString(timePartition));
      logStream.newLine();
    }
    logStream.write(FLAG_TIME_PARTITIONS_HEADER_DONE);
    logStream.newLine();
    logStream.flush();
  }

  public void startTimePartition(
      List<TsFileResource> selectedFiles, long timePartition, boolean isSeq) throws IOException {
    if (selectedFiles == null || selectedFiles.isEmpty()) {
      throw new IOException("selectedFiles is null or empty");
    }
    logStream.write(
        FLAG_TIME_PARTITION_START
            + TsFileIdentifier.INFO_SEPARATOR
            + timePartition
            + TsFileIdentifier.INFO_SEPARATOR
            + (isSeq ? FLAG_SEQ : FLAG_UNSEQ));
    logStream.newLine();
    for (TsFileResource tsFileResource : selectedFiles) {
      logStream.write(
          FLAG_INIT_SELECTED_FILE
              + TsFileIdentifier.INFO_SEPARATOR
              + TsFileIdentifier.getFileIdentifierFromFilePath(
                      tsFileResource.getTsFile().getAbsolutePath())
                  .toString());
      logStream.newLine();
    }
    logStream.flush();
  }

  public void doneFile(TsFileResource file) throws IOException {
    if (file == null) {
      throw new IOException("file is null");
    }
    logStream.write(
        FLAG_DONE
            + TsFileIdentifier.INFO_SEPARATOR
            + TsFileIdentifier.getFileIdentifierFromFilePath(file.getTsFile().getAbsolutePath())
                .toString());
    logStream.newLine();
    logStream.flush();
  }

  public void endTimePartition(long timePartition) throws IOException {

    logStream.write(FLAG_TIME_PARTITION_DONE);
    logStream.newLine();
    logStream.write(Long.toString(timePartition));
    logStream.newLine();
    logStream.flush();
  }

  public static File findAlterLog(String directory) {
    File dataRegionDir = new File(directory);
    if (dataRegionDir.exists()) {
      File[] files = dataRegionDir.listFiles((dir, name) -> name.equals(ALTERING_LOG_NAME));
      if (files == null || files.length == 0) {
        return null;
      }
      return files[0];
    } else {
      return null;
    }
  }
}
